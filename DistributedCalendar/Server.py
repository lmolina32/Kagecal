import sys
import os
import json
import socket
import selectors
import pickle
import logging
import threading
from enum import Enum, IntEnum
from typing import Optional, Callable, Any, TypedDict

from .Client import Client
from .PersistantCalendar import PersistantCalendar
from .Calendar import Calendar, Repeats, Event

type Socket = socket.socket

# TODO: update logging to be from the central invocation not called in every sub module.
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s %(asctime)s %(module)s:%(lineno)d] %(message)s",
    datefmt="%H:%M:%S",
)

# TODO: Server need to send UDP broadcasts containing the current logical clock after every calendar state mutation, as well as every interval.


class ServerMode(Enum):
    FOLLOWER = 0
    LEADER = 1


class ServerFlags(IntEnum):
    NONE = 0
    DO_SYNC = 1 << 0
    """Indicates that the peer should sync with the leader."""
    DO_ELECTION = 1 << 1
    """Indicates that the peer should start an election"""
    NEW_LEADER = 1 << 2
    """Indicates that the peer should replace its leader client with a new one by reading the updated leader endpoint on the server.."""
    DO_BROADCAST = 1 << 3
    """Indicates that the server (in leader mode) should broadcast its logical clock."""
    COORDINATE_RECVD = 1 << 4
    """Indicates that the server got a coordinate message."""


class RPC(TypedDict):
    method: str
    peer_ident: str
    params: dict[str, str | int]


class Server:
    BUFFER_SIZE = 1 << 12  # 4 KiB
    CLIENT_SOCK_TIMEOUT = 5  # Seconds
    NAMESERV_KEEPALIVE = 60  # Seconds
    CLOCK_BROADCAST = 60  # Seconds
    BROADCAST_PORT = 9375
    BROADCAST_MAXLEN = 1 << 10
    MAX_CONCURRENCY = 100

    def __init__(
        self,
        calendar_ident: str,
        peer_ident: str,
        ckpt_path: str,
        txn_path: str,
        leader_host: str,
        leader_port: int,
    ):
        # Logging
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.DEBUG)

        # Set up calednar and its lock
        self.persistence = PersistantCalendar(ckpt_path, txn_path)
        self.calendar_lock = threading.Lock()

        # Init server state
        self.calendar_ident: str = calendar_ident
        self.peer_ident: str = peer_ident
        self.leader_host = leader_host
        self.leader_port = leader_port
        self.coordinate = False
        self.RPC_METHODS: dict[str, Callable[[str, dict], tuple]] = {
            "create": self._create,
            "delete": self._delete,
            "modify": self._modify,
            "who_is_leader": self._who_is_leader,
            "coordinate": self._coordinate,
            "election": self._election,
        }

        self.mode: ServerMode = ServerMode.FOLLOWER
        self.mode_lock = threading.Lock()

        # Initialize server socket and socket selector.
        servsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        servsock.bind((socket.gethostname(), 0))
        servsock.listen(self.MAX_CONCURRENCY)
        self.host, self.port = servsock.getsockname()

        # Set up UDP broadcast sockets.
        self.broadcast_sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.broadcast_sender.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        broadcast_receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        broadcast_receiver.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        broadcast_receiver.bind(("", self.BROADCAST_PORT))

        self.log.info(
            f"{self.peer_ident} listening at \033[32m{self.host} {self.port}\033[0m"
        )

        # Set up socket selector.
        # See https://docs.python.org/3/library/selectors.html
        self.sock_selector = selectors.DefaultSelector()
        self.sock_selector.register(servsock, selectors.EVENT_READ, self._accept)
        self.sock_selector.register(
            broadcast_receiver, selectors.EVENT_READ, self._handle_broadcast
        )

        # Set up name service daemon
        self.stop: threading.Event = threading.Event()
        self.threads: list[threading.Thread] = []
        t = threading.Thread(
            target=self._name_server,
            daemon=True,
        )
        t.start()
        self.threads.append(t)

    def __del__(self) -> None:
        # Close all sockets registered with the socket selector (which should be all of them)
        for sock, _ in self.sock_selector.get_map():
            self._close_socket(sock)

        self.sock_selector.close()

        # Join all threads.
        self.stop.set()
        for t in self.threads:
            t.join()

    def serve(self) -> int:
        """Poll all client connections for incoming requests and serve them. Returns after one round of socket events has been handled."""
        with self.calendar_lock:
            server_flags = 0
            for key, mask in self.sock_selector.select():
                callback = key.data
                server_flags |= callback(key.fileobj)
        return server_flags

    # === SOCKET MULTIPLEXING ===
    def _accept(self, servsock: Socket) -> int:
        """Socket selector callback that handles an incoming connection on the server socket."""
        clientsock, addr = servsock.accept()
        self.log.info(f"Accepted {clientsock} from {addr}.")
        clientsock.settimeout(self.CLIENT_SOCK_TIMEOUT)
        self.sock_selector.register(clientsock, selectors.EVENT_READ, self._handle_rpc)
        return 0

    def _handle_rpc(self, clientsock: Socket) -> int:
        """Socket selector callback that handles an incoming RPC event from a registered client socket."""
        # 1. Attempt to read in the entire RPC. If we get zero, have to close and unregister the client socket.
        try:
            request = self._get_rpc(clientsock)
        except ValueError as e:
            self.log.error(e)
            self.sock_selector.unregister(clientsock)
            self._close_socket(clientsock)
            return ServerFlags.NONE

        # 2. Parse the request.
        method = request["method"]
        params = request["params"]
        from_host, from_port = clientsock.getpeername()
        self.log.info(f"Received RPC [{method}] from {request["peer_ident"]}.")

        if self.coordinate:
            # Block until coordinate is seen.
            if method == "coordinate":
                response, flags = self._coordinate(method, params)
                self.set_coordinate(False)
            else:
                response, flags = {"status": "coordinate"}, ServerFlags.NONE
        else:
            match self.mode:
                case ServerMode.FOLLOWER:
                    if method in {"who_is_leader", "coordinate", "election"}:
                        response, flags = self.RPC_METHODS[method](method, params)
                    else:
                        response, flags = {
                            "status": "redirect",
                            "host": self.leader_host,
                            "port": self.leader_port,
                        }, ServerFlags.NONE
                case ServerMode.LEADER:
                    try:
                        response, flags = self.RPC_METHODS[method](method, params)
                    except ValueError as e:
                        self.log.error(f"{e}")
                        response, flags = {
                            "status": "failure",
                            "error": str(e),
                        }, ServerFlags.NONE

        # 3. Send ack.
        pickled_msg = pickle.dumps(response)
        header = str(len(pickled_msg)).encode() + b"\n"
        try:
            clientsock.sendall(header + pickled_msg)
        except BrokenPipeError | ConnectionResetError as e:
            self.log.warn(f"Ack to {clientsock} failed: {e}")
            self.sock_selector.unregister(clientsock.fileno())
            self._close_socket(clientsock)
        except socket.timeout:
            self.log.warn(f"Ack to {clientsock} timed out.")

        # Broadcast updated logical clock if necessary
        if self.mode == ServerMode.LEADER and method in {"create", "delete", "modify"}:
            self.broadcast_clock()

        return flags

    def _get_rpc(self, clientsock: Socket, use_timeout: bool = True) -> RPC:
        """Gets an RPC message from the client, using a configurable timeout. On failure, raises a ValueError"""
        # Recieve Header
        header = b""
        while b"\n" not in header:
            try:
                data = clientsock.recv(self.BUFFER_SIZE)
            except socket.timeout:
                raise ValueError("Timeout exceeded on call to recv.")
            header += data

        delim_idx = header.index(b"\n")
        data_size = int(header[:delim_idx].decode())
        leftover = header[delim_idx + 1 :]
        buffer = [leftover]
        read_amt = len(leftover)

        # Recieve body
        while read_amt < data_size:
            try:
                data = clientsock.recv(self.BUFFER_SIZE)
            except socket.timeout:
                raise ValueError("Timeout exceeded on call to recv.")
            read_amt += len(data)
            buffer.append(data)

        try:
            request = pickle.loads(b"".join(buffer))
        except pickle.UnpicklingError:
            raise ValueError(f"Deserialization failed from {clientsock}")

        self._validate_rpc(request)
        return request

    def _handle_broadcast(self, receiver: Socket) -> int:
        """Handles an incoming broadcast from the leader containing its logical clock. If the clock is higher than this peer's clock, inform the peer that we need to sync with the leader."""
        data, addr = receiver.recvfrom(self.BROADCAST_MAXLEN)
        match addr:
            case (self.host, self.port):
                # This node is the leader, and the broadcast came from itself.
                return 0
            case (self.leader_host, self.leader_port):
                # Broacast came from leader. Check if sync necessary
                # if addr == self.leaders_address:
                try:
                    message = json.loads(data)
                except json.decoder.JSONDecodeError:
                    return 0
                if message.get("calendar_ident") != self.calendar_ident:
                    return 0
                clock = message.get("logical_clock", 0)
                return (
                    ServerFlags.DO_SYNC
                    if clock > self.persistence.get_logical_clock()
                    else 0
                )
            case _:
                # Came from some other node. Ignore.
                return 0

    def broadcast_clock(self) -> None:
        """Broadcasts to all nodes a dict of the form {"calendar_ident": str, "logical_clock": int }. The calendar ident is to prevent cross talk from several concurrently running calendars."""
        # TODO: This could cause a problem if the calendar ident is sufficiently long that it exceeds the MTU of the network nodes, causing the UDP packet to be fragmented and possibly arrive out of order.
        message = {
            "calendar_ident": self.calendar_ident,
            "logical_clock": self.persistence.get_logical_clock(),
        }
        message_bytes = json.dumps(message).encode("utf-8")
        try:
            self.broadcast_sender.sendto(
                message_bytes, ("<broadcast>", self.BROADCAST_PORT)
            )
        except OSError:
            self.log.warn("Failed to broadcast clock.")

    def _close_socket(self, sock: Socket) -> None:
        """Close file descriptor socket gracefully"""
        try:
            sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        sock.close()

    def _name_server(
        self,
    ) -> None:
        """Deamon thread target that periodically register this server with the ND catalog via UDP."""
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        hostname: str = "catalog.cse.nd.edu"
        port: int = 9097
        raw_data: dict[str, str | int] = {
            "owner": "Sam, Leo",
            "project": "kagecal",
            "calendar_ident": self.calendar_ident,
            "peer_ident": self.peer_ident,
            "port": self.port,
            "host": self.host,
            "PID": os.getpid(),
        }
        data = json.dumps(raw_data).encode()
        data_size = len(data)
        while not self.stop.is_set():
            sent_amt = 0
            while sent_amt < data_size:
                sent = s.sendto(data[: data_size - sent_amt], (hostname, port))
                sent_amt += sent
            self.log.info(f"Send UDP packet for naming to {hostname}")
            self.stop.wait(self.NAMESERV_KEEPALIVE)
        s.close()

    # === RPC Handlers ===
    def _create(self, method: str, params: dict) -> tuple[dict, ServerFlags]:
        ident = self.persistence.create(**params)
        if ident is None:
            raise ValueError(f"{method} did not create Event on shared calendar")
        return {"method": method, "status": "success", "ident": ident}, ServerFlags.NONE

    def _delete(self, method: str, params: dict) -> tuple[dict, ServerFlags]:
        self.persistence.delete(**params)
        return {"method": method, "status": "success"}, ServerFlags.NONE

    def _modify(self, method: str, params: dict) -> tuple[dict, ServerFlags]:
        ident = self.persistence.modify(**params)
        if ident is None:
            raise ValueError(f"{method} did not modify Event on shared calendar")
        return {"method": method, "status": "success", "ident": ident}, ServerFlags.NONE

    def _who_is_leader(self, method: str, params: dict) -> tuple[dict, ServerFlags]:
        """RPC method that responds with the endpoint of the current leader."""
        match self.mode:
            case ServerMode.LEADER:
                return {
                    "method": method,
                    "status": "success",
                    "host": self.host,
                    "port": self.port,
                }, ServerFlags.NONE
            case ServerMode.FOLLOWER:
                return {
                    "method": method,
                    "status": "success",
                    "host": self.leader_host,
                    "port": self.leader_port,
                }, ServerFlags.NONE

    def _sync(self, method: str, params: dict) -> tuple[dict, ServerFlags]:
        """RPC handler for SYNC requests. If the server is the leader or the server is a follower and the requesting client is the leader, sends the server's entire calendar state and logical clock to the client."""
        return {
            "method": method,
            "status": "success",
            "calendar": self.persistence.list_events(),
            "logical_clock": self.persistence.get_logical_clock(),
        }, ServerFlags.NONE

    def _coordinate(self, method: str, params: dict) -> tuple[dict, ServerFlags]:
        """RPC handler that responds to COORDINATE messages. Updates the local leader endpoint and responds with logical clock value."""
        self.leader_host = params["host"]
        self.leader_port = params["port"]
        with self.mode_lock:
            self.mode = ServerMode.FOLLOWER
        return {
            "method": method,
            "status": "success",
            "logical_clock": self.persistence.get_logical_clock(),
        }, ServerFlags.NEW_LEADER

    def _election(self, method: str, params: dict) -> tuple[dict, ServerFlags]:
        """RPC handler that responds to ELECTION messages. Responds with an OK and sets the DO_ELECTION flag."""
        return {
            "method": method,
            "status": "success",
        }, ServerFlags.DO_ELECTION

    def _validate_rpc(self, rpc: RPC) -> None:
        """Raises a ValueError if params is an invalid RPC."""
        return
        # TODO: Update this function.
        if "params" not in rpc or not rpc["params"]:
            raise ValueError(
                f"{method} parameters empty, look at API for specific paramters"
            )

        if not isinstance(params, dict):
            raise ValueError(f"Parameters must be passed in as a dictionary")

        if (method in {"delete", "modify", "get_events"}) and "ident" not in params:
            raise ValueError(f"{method} requires the parameter ident")

        if method == "create" or method == "modify":
            if "name" not in params:
                raise ValueError(f"{method} requires the parameter name")
            if "start" not in params:
                raise ValueError(f"{method} requires the parameter start")
            if "end" not in params:
                raise ValueError(f"{method} requires the parameter end")

    def set_coordinate(self, value: bool):
        """Sets the coordinate flag. If the flag is True, the server will only respond to COORDINATE messages, and will ignore all others."""
        self.coordinate = value

    def update(self, events: dict[int, Event], logical_clock: int):
        """Updates the calendar state to match the passed in event list and clock. Analogous to _sync."""
        with self.calendar_lock:
            self.persistence.update(events, logical_clock)

    def set_mode(self, mode: ServerMode) -> None:
        """Sets the server mode to either FOLLOWER or LEADER."""
        with self.mode_lock:
            self.mode = mode


def main() -> None:
    if len(sys.argv) != 3:
        print("Usage: python ./Server.py calendar_ident peer_ident")
        sys.exit(1)
    calendar_ident = sys.argv[1]
    peer_ident = sys.argv[2]
    ckpt: str = f"calendar_{calendar_ident}_{peer_ident}.ckpt"
    txn: str = f"calendar_{calendar_ident}_{peer_ident}.txn"
    server = Server(
        calendar_ident=calendar_ident,
        peer_ident=peer_ident,
        ckpt_path=ckpt,
        txn_path=txn,
        leader_host="",
        leader_port=0,
    )

    try:
        while True:
            server.serve()
    except KeyboardInterrupt:
        server.log.info(f"\n{'-'*50}\nShutting down server")


if __name__ == "__main__":
    main()
