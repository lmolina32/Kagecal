#!/usr/bin/env python3

from __future__ import annotations

import pdb
import sys
import json
import socket
import select
import pickle
import logging
import threading
from enum import Enum
from typing import Optional, Callable

from .Client import Client
from .Calendar import Calendar, Repeats, Event
from .PersistantCalendar import PersistantHashTable


# TODO: update logging to be from the central invocation not called in every sub module.
logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s %(asctime)s %(module)s:%(lineno)d] %(message)s",
    datefmt="%H:%M:%S",
)


class ServerMode(Enum):
    FOLLOWER = 0
    LEADER = 1


# TODO: update type hints
class Server:
    BUFFER_SIZE = 1 << 12

    def __init__(
        self,
        project_name: str,
        server_name: str,
        ckpt_path: str,
        txn_path: str,
        port: int = 0,
    ):
        self.project_name: str = project_name
        self.server_name: str = server_name
        self.port: int = port
        self.host: str = socket.gethostname()
        self.socket: Optional[socket.socket] = None
        self.client_sockets: dict[int, socket.socket] = {}
        """Maps a epoll file descriptor to a socket."""
        self.client_addresses: dict[int, tuple[str, int]] = {}
        """Maps a epoll file descriptor to client endpoint."""
        self.threads: list[threading.Thread] = []
        self.stop: threading.Event = threading.Event()
        self.epoll: Optional[select.epoll] = None
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.DEBUG)
        self.persistence = PersistantHashTable(
            ckpt_path=ckpt_path, txn_log_path=txn_path
        )
        # TODO: upon sync, peer sends the leader their own host, port
        # TODO: add logic for elections maybe need attributes (synced peers, queue for seralization??)
        self.followers: list[tuple[int, str]] = []
        self.mode: ServerMode = ServerMode.FOLLOWER
        self.leaders_address: tuple[int, str] = ("", 0)

        self.map_to_method: dict[str, Callable[[str, dict], dict]] = {
            "create": self._create,
            "delete": self._delete,
            "modify": self._modify,
            "get_event": self._get_event,
            "list_events": self._list_events,
            "who_is_leader": self._who_is_leader,
            "register_and_sync": self._register_and_sync,
        }
        # self.logical_clock: int = self.persistence.logical_clock

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            # set up server socket
            self.socket.bind(("", self.port))
            self.socket.listen(10)
            self.port = self.socket.getsockname()[1]
            self.socket.setblocking(False)
            self.log.info(f"{self.server_name} listening on \033[32m{self.port}\033[0m")

            # set up name service daemon
            t = threading.Thread(
                target=self.name_server,
                daemon=True,
            )
            t.start()
            self.threads.append(t)

            self.epoll = select.epoll()
            if self.epoll is None:
                raise ValueError("Epoll object not initialized")
            self.epoll.register(self.socket.fileno(), select.EPOLLIN)
        except Exception as e:
            self.log.error(f"Could not start the Server: {e}")
            sys.exit(1)

    def _cleanup(self) -> None:
        """Shutdown server gracefully"""
        if self.epoll is not None:
            try:
                if self.socket:
                    self.epoll.unregister(self.socket.fileno())
            except Exception:
                pass
            self.epoll.close()
            self.epoll = None
        self._close_server_socket()
        self.stop.set()
        for t in self.threads:
            t.join()

    def _handle_events(self, timeout: int = 1) -> None:
        """Handle events for each client that reaches out to the server"""
        if self.epoll is None:
            raise ValueError("Epoll object not initialized")
        if self.socket is None:
            raise ValueError("Socket is not initialized")

        events = self.epoll.poll(timeout)
        for fileno, event in events:
            # new connections from client
            if fileno == self.socket.fileno():
                clt_socket, clt_addr = self.socket.accept()
                clt_socket.setblocking(False)
                clt_fileno = clt_socket.fileno()
                self.epoll.register(clt_fileno, select.EPOLLIN)
                self.client_sockets[clt_fileno] = clt_socket
                self.client_addresses[clt_fileno] = clt_addr
                self.log.info(f"Connection from {clt_addr}")
            # broken connection from client
            elif event & select.EPOLLHUP:
                self.log.info(
                    f"closing socket from {self.client_addresses.get(fileno, 'Unknown')}"
                )
                self._unregister_socket(fileno)
            # Receiving data from client
            elif event & select.EPOLLIN:
                try:
                    request = self._recv_all(fileno)
                    if request is None:
                        self._unregister_socket(fileno)
                        continue
                    if request == b"":
                        continue
                    # if not isinstance(request, dict):
                    #     continue
                    response = self._parse_request(request, fileno)
                    self._send_ack(response, fileno)
                except Exception as e:
                    self.log.error(f"{e}")
                    self.log.info(
                        f"closing socket from {self.client_addresses.get(fileno, 'Unknown')}"
                    )
                    self._unregister_socket(fileno)

    def _unregister_socket(self, fileno: int) -> None:
        """Remove connected file descriptors from the interest list, ensuring the kernel stops monitoring it for events"""
        if self.epoll is None:
            return
        try:
            self.epoll.unregister(fileno)
        except Exception:
            pass
        self._close_client_socket(fileno)
        self.client_addresses.pop(fileno, None)
        self.client_sockets.pop(fileno, None)

    def _recv_all(self, fileno: int) -> dict[str, str] | bytes | None:
        """Recieve entire payload from file descriptor of interest, return the payload if not malformed or broken connection"""
        # if fileno not in self.client_sockets:
        #     self.log.error(f"recv_all: {fileno} not in client sockets")
        #     return {}
        client_socket = self.client_sockets[fileno]
        client_address = self.client_addresses[fileno]
        header = b""

        # Recieve Header
        while b"\n" not in header:
            try:
                data = client_socket.recv(self.BUFFER_SIZE)
                if not data:
                    self.log.error(f"Connection broken from {client_address}")
                    return None
                header += data
            except BlockingIOError:
                if header:
                    continue
                return b""

        delim_idx = header.index(b"\n")
        data_size = int(header[:delim_idx].decode())
        buffer = header[delim_idx + 1 :]
        read_amt = len(buffer)

        # Recieve body
        while read_amt < data_size:
            try:
                data = client_socket.recv(self.BUFFER_SIZE)
                if not data:
                    self.log.error(
                        f"Connection broken mid-payload from {client_address}"
                    )
                    return None
                read_amt += len(data)
                buffer += data
            except BlockingIOError:
                continue

        try:
            return pickle.loads(buffer)
        except Exception as e:
            self.log.error(
                f"recv_all: Deserialization failed from {client_address}: {e}"
            )
            return None

    def _send_ack(self, payload: dict[str, str], fileno: int) -> None:
        """Send acknowledgment of the request to file descriptor"""
        # if fileno not in self.client_sockets:
        #     self.log.error(f"send_ack: {fileno} not in client sockets")
        #     return
        client_socket = self.client_sockets[fileno]
        pickled_msg = pickle.dumps(payload)
        header = str(len(pickled_msg)).encode() + b"\n"
        client_socket.setblocking(True)
        try:
            client_socket.sendall(header + pickled_msg)
        finally:
            client_socket.setblocking(False)

    def _parse_request(
        self, request: dict[str, str], fileno: int
    ) -> dict[str, str | int | dict[int, Event]]:
        # if fileno not in self.client_sockets:
        #     self.log.error(f"parse_request: {fileno} not in client sockets")
        #     return {"status": "failure", "error": f"{fileno} not in client sockets"}

        # if not isinstance(request, dict):
        #     self.log.info(
        #         f"Request was malformed from {self.client_addresses.get(fileno, "unknown")}"
        #     )
        #     return {
        #         "status": "failure",
        #         "error": "malformed payload: expected dict",
        #     }

        # TODO: add logic, leader does all of the below, follower only allows reads and rejects everything else
        try:
            # TODO: Idea add message_from or from key in dictionary, that has address, if the address is from a leader, receivec by a follower, then you know to add it to your calendar.
            # pdb.set_trace()
            method = request.get("method", "")
            params = request.get("params", {})
            msg_from = request.get("from", ())
            self.log.info(
                f"here is the method: {method}, leader address: {self.leaders_address} mine is {msg_from}"
            )
            if self.mode == ServerMode.FOLLOWER and msg_from != self.leaders_address:
                if method in ["create", "modify", "delete"]:
                    # TODO: need to add who_is_leader + register_and_sync when election
                    return {
                        "Status": "failure",
                        "error": "Not the leader, send all requests to leader",
                    }
            func = self.map_to_method.get(method, None)
            if func is None:
                self.log.info(f"Unknown method from {self.client_addresses[fileno]}")
                return {"status": "failure", "error": "error: Unrecognized method"}
            return func(method, params)
        except Exception as e:
            self.log.error(f"{e}")
            return {"status": "failure", "error": str(e)}

    def _create(self, method: str, params: dict) -> dict:
        valid, msg = self._validate_rpc(method, params)
        if not valid:
            return {"method": method, "status": "failure", "error": msg}
        ident = self.persistence.create(**params)
        if ident is None:
            return {"method": method, "status": "failure"}
        self.reverse_sync(method, params)
        return {"method": method, "status": "success", "ident": ident}

    def _delete(self, method: str, params: dict) -> dict:
        valid, msg = self._validate_rpc(method, params)
        if not valid:
            return {"method": method, "status": "failure", "error": msg}
        self.persistence.delete(**params)
        self.reverse_sync(method, params)
        return {"method": method, "status": "success"}

    def _modify(self, method: str, params: dict) -> dict:
        valid, msg = self._validate_rpc(method, params)
        if not valid:
            return {"method": method, "status": "failure", "error": msg}
        ident = self.persistence.modify(**params)
        if ident is None:
            return {"method": method, "status": "failure"}
        self.reverse_sync(method, params)
        return {"method": method, "status": "success", "ident": ident}

    def _get_event(self, method: str, params: dict) -> dict:
        valid, msg = self._validate_rpc(method, params)
        if not valid:
            return {"method": method, "status": "failure", "error": msg}
        event = self.persistence.get_event(**params)
        if event is None:
            return {"method": method, "status": "failure"}
        return {"method": method, "status": "success", "event": event}

    def _list_events(self, method: str, params: dict) -> dict:
        return {
            "method": method,
            "status": "success",
            "calendar": self.persistence.list_events(),
        }

    def _who_is_leader(self, method: str, params: dict) -> dict:
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

    def _register_and_sync(self, method: str, params: dict) -> dict:
        valid, msg = self._validate_rpc(method, params)
        if not valid:
            return {"method": method, "status": "failure", "error": msg}
        self.followers.append((params["host"], params["port"]))
        self.log.info(f"adding {params["host"]}:{params["port"]} to know peers")
        return {
            "method": method,
            "status": "success",
            "logical_clock": self.persistence.logical_clock,
            "calendar": self.persistence.list_events(),
        }

    def _validate_rpc(
        self, method: str, params: dict[str, str | int | Repeats | None]
    ) -> tuple[bool, str]:
        if not params:
            return (
                False,
                f"{method} parameters empty, look at API for specific paramters",
            )
        if not isinstance(params, dict):
            return False, f"Parameters must be passed in as a dictionary"

        if (method in {"delete", "modify", "get_events"}) and "ident" not in params:
            return False, f"{method} requires the parameter ident"

        if method == "create" or method == "modify":
            if "name" not in params:
                return False, f"{method} requires the parameter name"
            if "start" not in params:
                return False, f"{method} requires the parameter start"
            if "end" not in params:
                return False, f"{method} requires the parameter end"

        if method == "register_and_sync":
            if params.get("host", None) is None:
                return False, f"{method} requires the parameter host"
            if params.get("port", None) is None:
                return False, f"{method} requires the parameter port"
        return True, ""

    def reverse_sync(self, method: str, params: Dict) -> None:
        # TODO: this spawns _sync as a daemon thread in the background
        self.log.info("starting here ")
        self.log.info(f"{method}, {params}")
        t = threading.Thread(
            target=self._sync,
            args=(
                method,
                params,
                # Current Idea: leader has whole view of system, any new peers that join after this broadcast will already be sending a full sync to the leader either way
                self.followers.copy(),
            ),
        )
        t.start()
        self.threads.append(t)

    def _sync(self, method: str, params: Dict, followers: List) -> None:
        # TODO: pings all known peers in the system with the updated logical clock + CRUD operation
        for host, port in followers:
            self.log.info(
                f"{host}:{port} -> sending packet please work {method}\n\t{params}"
            )
            try:
                with Client(
                    self.server_name,
                    host=host,
                    port=port,
                    own_port=self.port,
                    own_host=self.host,
                ) as client:
                    self.log.info(
                        f"This is the method here {method}, {host}, {port}, {self.port}, {self.host}"
                    )
                    if method == "create":
                        client.create(**params)
                    elif method == "modify":
                        client.modify(**params)
                    elif method == "delete":
                        client.delete(**params)
            except Exception as e:
                self.log.info(f"Failed to send resync to {host}:{port}")
                self.log.info("here is the expection ", e)

    def _close_client_socket(self, fileno: int) -> None:
        """Close file descriptor socket gracefully"""
        if fileno not in self.client_sockets:
            self.log.error(f"close_client_socket: {fileno} not found in client sockets")
            return
        client_socket = self.client_sockets[fileno]
        if client_socket:
            try:
                client_socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            client_socket.close()

    def _close_server_socket(self) -> None:
        """Close the server socket gracefully"""
        if self.socket:
            try:
                self.socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.socket.close()
            self.socket = None

    def name_server(
        self,
    ) -> None:
        """Periodically register this server with the ND catalog via UDP."""
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        hostname: str = "catalog.cse.nd.edu"
        port: int = 9097
        raw_data: dict[str, str | int] = {
            "type": "calendar",
            "owner": "lmolina3",
            "port": self.port,
            "host": self.host,
            "project": self.project_name,
            "peer_name": self.server_name,
        }
        data = json.dumps(raw_data).encode()
        data_size = len(data)
        while not self.stop.is_set():
            sent_amt = 0
            while sent_amt < data_size:
                sent = s.sendto(data[: data_size - sent_amt], (hostname, port))
                sent_amt += sent
            self.log.info(f"Send UDP packet for naming to {hostname}")
            self.stop.wait(60)
        s.close()


def main() -> None:
    if len(sys.argv) != 3:
        print(f"Usage: python {sys.argv[0]} <project_name> <server_name>")
        sys.exit(1)
    project_name = sys.argv[1]
    server_name = sys.argv[2]
    ckpt: str = f"calendar_{project_name}_{server_name}.ckpt"
    txn: str = f"calendar_{project_name}_{server_name}.txn"
    server = Server(
        project_name=project_name, server_name=server_name, ckpt_path=ckpt, txn_path=txn
    )

    try:
        while True:
            # TODO: can add election logic here potentially (e.g handle events then handle election)
            server._handle_events()
    except KeyboardInterrupt:
        server.log.info(f"\n{'-'*50}\nShutting down server")
    finally:
        server._cleanup()


if __name__ == "__main__":
    main()
