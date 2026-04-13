#!/usr/bin/env python3 

from __future__ import annotations

import sys 
import json 
import socket 
import select
import pickle 
import logging 
import threading 
from queue import Queue 
from typing import Optional, Tuple, List, Dict
from Calendar import Calendar
from PersistantCalendar import PersistantHashTable 
# TODO: import persistent calendar and general calendar 

# TODO: update logging to be from the central invocation not called in every sub module. 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(peer_name)s : %(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

def name_server(
    stop_event: threading.Event,
    server_port: int,
    project_name: str,
    peer_name: str,
    log: logging.Logger,
) -> None:
    """Periodically register this server with the ND catalog via UDP."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    hostname: str = "catalog.cse.nd.edu"
    port: int = 9097
    raw_data: Dict[str, str | int] = {
        "type": "hashtable",
        "owner": "lmolina3",
        "port": server_port,
        "project": project_name,
        "peer_name": peer_name,
    }
    data = json.dumps(raw_data).encode()
    data_size = len(data)
    while not stop_event.is_set():
        sent_amt = 0
        while sent_amt < data_size:
            sent = s.sendto(data[: data_size - sent_amt], (hostname, port))
            sent_amt += sent
        log.info(f"Send UDP packet for naming to {hostname}")
        stop_event.wait(60)
    s.close()


# TODO: update type hints
class Server:
    BUFFER_SIZE = 2**10
    MAX_ENTRIES = 100 
    
    def __init__(self, project_name: str, server_name: str, port: int = 0):
        self.project_name: str = project_name
        self.server_name: str = server_name 
        self.port: int = port 
        self.socket: Optional[socket.socket] = None 
        self.client_sockets: dict[int, socket.socket] = {}
        self.client_addresses: dict[int, Tuple[str, int]] = {}
        self.threads: List[threading.Thread] = []
        self.stop: threading.Event = threading.Event()
        self.epoll: Optional[select.poll] = None
        log = logging.getLogger(__name__)
        self.log: logging.Logger = logging.LoggerAdapter(log, {"peer_name": server_name})
        self.log.setLevel(logging.DEBUG)

        self.calendar = Calendar()
        self.persistence = PersistantHashTable()
        # TODO: add presistence calendar
        # TODO: add logic for elections maybe need attributes (synced peers, queue for seralization??)
        
    def start(self) -> None:
        """Initilize server by binding to a port, spawning daemon for nameserver, and listening for requests"""
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
                target=name_server,
                args=(
                    self.stop,
                    self.port,
                    self.project_name,
                    self.server_name,
                    self.log
                ),
                daemon=True
            )
            t.start()
            self.threads.append(t)
            
            self.epoll = select.epoll()
            assert self.epoll is not None
            self.epoll.register(self.socket.fileno(), select.EPOLLIN)
        except Exception as e:
            self.log.error(f"start: {e}")
            sys.exit(1)

    def run(self) -> None:
        """Run server to handle reqeusts"""
        if not self.socket:
            self.start()

        try:
            while True:
                #TODO: can add election logic here potentially (e.g handle events then handle election)
                self._handle_events()
        except KeyboardInterrupt:
            self.log.info(f"\n{'-'*50}\nShutting down server")
        finally:
            self._cleanup()
            
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
    
    def _handle_events(self, timeout: int=1) -> None:
        """Handle events returned by epoll"""
        events = self.epoll.poll(timeout)
        for fileno, event in events:
            if fileno == self.socket.fileno():
                clt_socket, clt_addr = self.socket.accept()
                clt_socket.setblocking(False)
                clt_fileno = clt_socket.fileno()
                self.epoll.register(clt_fileno, select.EPOLLIN)
                self.client_sockets[clt_fileno] = clt_socket
                self.client_addresses[clt_fileno] = clt_addr
                self.log.info(f"Connection from {clt_addr}")
            elif event & select.EPOLLHUP:
                self.log.info(
                    f"closing socket from {self.client_addresses.get(fileno, 'Unknown')}"
                )
                self._unregister_socket(fileno)
            elif event & select.EPOLLIN:
                try:
                    request = self._recv_all(fileno)
                    if request is None:
                        self._unregister_socket(fileno)
                        continue
                    if request == b"":
                        continue
                    response = self._parse_request(request, fileno)
                    self._send_ack(response, fileno)
                except Exception as e:
                    self.log.error(f"{e}")
                    self.log.info(
                        f"closing socket from {self.client_addresses.get(fileno, 'Unknown')}"
                    )
                    self._unregister_socket(fileno)

    
    def _unregister_socket(self, fileno: int) -> None:
        try:
            self.epoll.unregister(fileno)
        except Exception:
            pass
        self._close_client_socket(fileno)
        self.client_addresses.pop(fileno, None)
        self.client_sockets.pop(fileno, None) 

    def _recv_all(self, fileno: int)-> dict[str, str] | bytes | None:
        if fileno not in self.client_sockets:
            self.log.error(f"recv_all: {fileno} not in client sockets")
            return {}
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
        if fileno not in self.client_sockets:
            self.log.error(f"send_ack: {fileno} not in client sockets")
            return
        client_socket = self.client_sockets[fileno]
        pickled_msg = pickle.dumps(payload)
        header = str(len(pickled_msg)).encode() + b"\n"
        client_socket.setblocking(True)
        try:
            client_socket.sendall(header + pickled_msg)
        finally:
            client_socket.setblocking(False)


    def _parse_request(self, request: dict[str, str], fileno: int) -> dict[str, str]:
        if fileno not in self.client_sockets:
            self.log.error(f"parse_request: {fileno} not in client sockets")
            return {"status": "failure", "error": f"{fileno} not in client sockets"}

        if not isinstance(request, dict):
            self.log.info(
                f"Request was malformed from {self.client_addresses.get(fileno, "unknown")}"
            )
            return {
                "status": "failure",
                "error": "malformed payload: expected dict",
            }
        try:
            method = request.get("method", "")
            match method:
                case "create":
                    name = request.get("name")
                    start = request.get("start")
                    end = request.get("end")
                    description = request.get("description")
                    location = request.get("location")
                    repeats = request.get("repeats")
                    ident = self.persistence.create(name, start, end, description, location, repeats) 
                    if ident is None:
                        return {"method": "create", "status": "failure"}
                    return {"method": "create", "status": "success", "ident": ident}
                case "delete":
                    ident = request.get("ident")
                    self.persistence.delete(ident)
                    return {"method": "delete", "status": "success"}
                case "modify":
                    ident = request.get("ident")
                    name = request.get("name")
                    start = request.get("start")
                    end = request.get("end")
                    description = request.get("description")
                    location = request.get("location")
                    repeats = request.get("repeats")
                    ident = self.persistence.modify(ident, name, start, end, description, location, repeats) 
                    if ident is None:
                        return {"method": "modify", "status": "failure"}
                    return {"method": "modify", "status": "success", "ident": ident}

                case "get_event":
                    pass
                case "list_events":
                    pass
                case _:
                    self.log.info(
                        f"Unknown method from {self.client_addresses[fileno]}"
                    )
                    return {"status": "failure", "error": "error: Unrecognized method"}
        except Exception as e:
            self.log.error(f"{e}")
            return {"status": "failure", "error": str(e)}

    def _close_client_socket(self, fileno: int) -> None:
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
        if self.socket:
            try:
                self.socket.shutdown(socket.SHUT_RDWR)
            except OSError:
                pass
            self.socket.close()
            self.socket = None

            

def main() -> None:
    if len(sys.argv) != 3:
        print(f"Usage: python {sys.argv[0]} <project_name> <server_name>")
        sys.exit(1)
    project_name = sys.argv[1]
    server_name = sys.argv[2]
    server = Server(project_name=project_name, server_name=server_name)
    server.start()
    server.run()


if __name__ == "__main__":
    main()
