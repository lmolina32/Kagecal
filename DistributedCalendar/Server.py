import sys
import os
import json
import socket
import selectors
import pickle
import logging
import threading
from enum import Enum
from typing import Optional, Callable

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


class ServerFlags(Enum):
    DO_SYNC = 1 << 0
    DO_ELECTION = 1 << 1


# TODO: update type hints
class Server:
    BUFFER_SIZE = 1 << 12  # 4 KiB
    CLIENT_SOCK_TIMEOUT = 5  # Seconds
    NAMESERV_KEEPALIVE = 60  # Seconds
    CLOCK_BROADCAST = 60  # Seconds
    BROADCAST_PORT = 9375
    BROADCAST_MAXLEN = 1 << 10

    def __init__(
        self,
        calendar_ident: str,
        peer_ident: str,
        ckpt_path: str,
        txn_path: str,
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
        self.RPC_METHODS: dict[str, Callable[[str, dict], dict]] = {
            "create": self._create,
            "delete": self._delete,
            "modify": self._modify,
            "get_event": self._get_event,
            "list_events": self._list_events,
            "who_is_leader": self._who_is_leader,
            "register_and_sync": self._register_and_sync,
        }

        # TODO: upon sync, peer sends the leader their own host, port
        # TODO: add logic for elections maybe need attributes (synced peers, queue for seralization??)
        self.leaders_address: tuple[int, str] = ("", 0)
        self.mode: ServerMode = ServerMode.FOLLOWER
        self.mode_lock = threading.Lock()

        # Initialize server socket and socket selector.
        servsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        servsock.bind((socket.gethostname(), 0))
        servsock.listen(self.MAX_CONCURRENCY)

        # Set up UDP broadcast sockets.
        broadcast_sender = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        broadcast_sender.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        broadcast_receiver = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        receiver.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        receiver.bind(("", self.BROADCAST_PORT))

        self.host, self.port = servsock.getsockname()
        self.log.info(f"{self.peer_ident} listening on \033[32m{self.port}\033[0m")

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
                server_flags &= callback(key.fileobj)
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
        # TODO: IF we get an ELECTION message, respond with ELEC_OK and return DO_ELECTION
        # TODO: IF we get an COORDINATE message, respond with COORD_OK and clock, update leader endpoint.
        # TODO: IF we get a SYNC from the leader, let that go through.

        # 1. Attempt to read in the entire RPC. If we get zero, have to close and unregister the client socket.

        # Recieve Header
        header = b""
        while b"\n" not in header:
            try:
                data = clientsock.recv(self.BUFFER_SIZE)
            except socket.timeout:
                data = b""
            if not data:
                self.log.error(f"Connection broken from {clientsock}")
                self.sock_selector.unregister(clientsock)
                self._close_socket(clientsock)
                return 0
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
                data = b""
            data = clientsock.recv(self.BUFFER_SIZE)
            if not data:
                self.log.error(f"Connection broken mid-payload from {clientsock}")
                self.sock_selector.unregister(clientsock)
                self._close_socket(sock)
                return 0
            read_amt += len(data)
            buffer.append(data)

        try:
            request = pickle.loads(b"".join(buffer))
        except pickle.UnpicklingError as e:
            self.log.error(f"Deserialization failed from {client_address}: {e}")
            self.sock_selector.unregister(clientsock)
            self._close_socket(sock)
            return 0

        # 2. Parse the request.
        # TODO: add logic, leader does all of the below, follower only allows reads and rejects everything else
        try:
            # TODO: Idea add message_from or from key in dictionary, that has address, if the address is from a leader, receivec by a follower, then you know to add it to your calendar.
            method = request.get("method", "")
            params = request.get("params", {})
            msg_from = request.get("from", ())
            self.log.debug(
                f"here is the method: {method}, leader address: {self.leaders_address} mine is {msg_from}"
            )
            if self.mode == ServerMode.FOLLOWER and msg_from != self.leaders_address:
                if method in {"create", "modify", "delete"}:
                    # TODO: need to add who_is_leader + register_and_sync when election
                    # TODO: Instead of raising permission error, should inform sender of current leader.
                    raise PermissionError(
                        f"This peer is not the leader, send all (create, modify, delete) requests to leader"
                    )
            if method not in self.RPC_METHODS:
                raise ValueError(f"{method} is not a valid RPC method")
            func = self.RPC_METHODS[method]
            response = func(method, params)
        except ValueError | PermissionError as e:
            self.log.error(f"{e}")
            response = {"status": "failure", "error": str(e)}

        # 3. Send ack.
        self._send_ack(response, clientsock)
        return 0

    def _handle_broadcast(self, receiver: Socket) -> int:
        """Handles an incoming broadcast from the leader containing its logical clock. If the clock is higher than this peer's clock, inform the peer that we need to sync with the leader."""
        data, addr = receiver.recvfrom(self.BROADCAST_MAXLEN)
        match addr:
            case (self.host, self.port):
                # This node is the leader, and the broadcast came from itself.
                return 0
            case self.leaders_address:
                # Broacast came from leader. Check if sync necessary
                if addr == self.leaders_address:
                    try:
                        message = json.loads(data)
                    except json.decoder.JSONDecodeError:
                        return 0
                clock = message.get("logical_clock", 0)
                return ServerFlags.DO_SYNC if clock > self.logical_clock else 0
            case _:
                # Came from some other node. Ignore.
                return 0

    def _broadcast_clock(self, sender: Socket) -> None:
        """Broadcasts to all nodes a dict of the form {"calendar_ident": str, "logical_clock": int }. The calendar ident is to prevent cross talk from several concurrently running calendars."""
        # TODO: This could cause a problem if the calendar ident is sufficiently long that it exceeds the MTU of the network nodes, causing the UDP packet to be fragmented and possibly arrive out of order.
        message = {
            "calendar_ident": self.calendar_ident,
            "logical_clock": self.logical_clock,
        }
        message_bytes = json.dumps(message)
        try:
            sender.sendto(message_bytes, ("<broadcast>", self.BROADCAST_PORT))
        except OSError:
            self.log.warn("Failed to broadcast clock.")

    def _send_ack(self, payload: dict[str, str], clientsock: Socket) -> None:
        """Send acknowledgment of the request to file descriptor"""
        pickled_msg = pickle.dumps(payload)
        header = str(len(pickled_msg)).encode() + b"\n"
        try:
            clientsock.sendall(header + pickled_msg)
        except BrokenPipeError | ConnectionResetError as e:
            self.log.warn(f"Ack to {clientsock} failed: {e}")
            self.sock_selector.unregister(clientsock.fileno())
            self._close_socket(sock)
        except socket.timeout:
            self.log.warn(f"Ack to {clientsock} timed out.")

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
    def _create(self, method: str, params: dict) -> dict:
        self._validate_rpc(method, params)
        ident = self.persistence.create(**params)
        if ident is None:
            raise ValueError(f"{method} did not create Event on shared calendar")
        self.reverse_sync(method, params)
        return {"method": method, "status": "success", "ident": ident}

    def _delete(self, method: str, params: dict) -> dict:
        self._validate_rpc(method, params)
        self.persistence.delete(**params)
        self.reverse_sync(method, params)
        return {"method": method, "status": "success"}

    def _modify(self, method: str, params: dict) -> dict:
        self._validate_rpc(method, params)
        ident = self.persistence.modify(**params)
        if ident is None:
            raise ValueError(f"{method} did not modify Event on shared calendar")
        self.reverse_sync(method, params)
        return {"method": method, "status": "success", "ident": ident}

    def _get_event(self, method: str, params: dict) -> dict:
        self._validate_rpc(method, params)
        event = self.persistence.get_event(**params)
        if event is None:
            raise ValueError(
                f"{method} could not find event with identifier in shared calendar"
            )
        return {"method": method, "status": "success", "event": event}

    def _list_events(self, method: str, params: dict) -> dict:
        return {
            "method": method,
            "status": "success",
            "calendar": self.persistence.list_events(),
        }

    def _who_is_leader(self, method: str, params: dict) -> dict:
        """RPC method that responds with the endpoint of the current leader."""
        match self.mode:
            case ServerMode.LEADER:
                return {
                    "method": method,
                    "status": "success",
                    "host": self.host,
                    "port": self.port,
                }
            case ServerMode.FOLLOWER:
                return {
                    "method": method,
                    "status": "success",
                    "host": self.leaders_address[0],
                    "port": self.leaders_address[1],
                }

    def _validate_rpc(
        self, method: str, params: dict[str, str | int | Repeats | None]
    ) -> None:
        """Raises a ValueError if params is an invalid RPC."""
        if not params:
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

        if method == "register_and_sync":
            if "host" not in params:
                raise ValueError(f"{method} requires the parameter host")
            if "port" not in params:
                raise ValueError(f"{method} requires the parameter port")

    # def _register(self, method: str, params: dict) -> dict:
    #     """RPC method that adds the client's Peer to the list of known peers."""
    #     try:
    #         self._validate_rpc(method, params)
    #     except ValueError as e:
    #         return {"method": method, "status": "failure", "error": e}

    #     # TODO: Server should know the UDP broadcast receiver endpoint.
    #     self.followers.append((params["host"], params["port"]))
    #     self.log.info(f"adding {params["host"]}:{params["port"]} to know peers")
    #     return {
    #         "method": method,
    #         "status": "success",
    #         "logical_clock": self.persistence.logical_clock,
    #         "calendar": self.persistence.list_events(),
    #     }

    # def reverse_sync(self, method: str, params: dict) -> None:
    #     # TODO: this spawns _sync as a daemon thread in the background
    #     self.log.info("starting here ")
    #     self.log.info(f"{method}, {params}")
    #     t = threading.Thread(
    #         target=self._sync,
    #         args=(
    #             method,
    #             params,
    #             # Current Idea: leader has whole view of system, any new peers that join after this broadcast will already be sending a full sync to the leader either way
    #             self.followers.copy(),
    #         ),
    #     )
    #     t.start()
    #     self.threads.append(t)

    def _sync(self, method: str, params: dict) -> dict:
        """RPC handler for SYNC requests. If the server is the leader or the server is a follower and the requesting client is the leader, sends the server's entire calendar state and logical clock to the client."""
        return {
            "method": method,
            "status": "success",
            "calendar": self.persistence.list_events(),
            "logical_clock": self.logical_clock,
        }

        # TODO: Respond with the entire event list and logical clock.

        # TODO: pings all known peers in the system with the updated logical clock + CRUD operation
        # for host, port in followers:
        #     self.log.info(
        #         f"{host}:{port} -> sending packet please work {method}\n\t{params}"
        #     )
        #     try:
        #         with Client(
        #             self.peer_ident,
        #             host=host,
        #             port=port,
        #             own_port=self.port,
        #             own_host=self.host,
        #         ) as client:
        #             self.log.info(
        #                 f"This is the method here {method}, {host}, {port}, {self.port}, {self.host}"
        #             )
        #             if method == "create":
        #                 client.create(**params)
        #             elif method == "modify":
        #                 client.modify(**params)
        #             elif method == "delete":
        #                 client.delete(**params)
        #     except Exception as e:
        #         self.log.info(f"Failed to send resync to {host}:{port}")
        #         self.log.info("here is the expection ", e)

    def coordinate():
        """RPC handler that responsds to COORDINATE messages. Updates the local leader endpoint and responds with logical clock value."""
        pass

    def await_coordinate():
        """Same behavior as coordinate, but does a blocking read until a COORDINATE message is received."""
        pass

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
    )

    try:
        while True:
            # TODO: can add election logic here potentially (e.g handle events then handle election)
            server.serve()
    except KeyboardInterrupt:
        server.log.info(f"\n{'-'*50}\nShutting down server")


if __name__ == "__main__":
    main()
