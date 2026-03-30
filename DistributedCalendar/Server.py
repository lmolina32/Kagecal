#!/usr/bin/env python3 

import sys 
import json 
import socket 
import select
import pickle 
import logging 
import threading 
from queue import Queue 
from typing import Optional, Tuple, List
# TODO: import persistent calendar and general calendar 

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
    while stop_event.is_set():
        continue


# TODO: update type hints
class Server:
    BUFFER_SIZE = 2**10
    MAX_ENTRIES = 100 
    
    def __init__(self, project_name: str, server_name: str, port: Optional[int] = 0):
        self.project_name: str = project_name
        self.server_name: str = server_name 
        self.port: int = port 
        self.socket: socket.socket = None 
        self.client_sockets: dict[int, socket.socket] = {}
        self.client_addresses: dict[int, Tuple[str, int]] = {}
        self.threads: List = []
        self.stop: threading.Event = threading.Event()
        self.epoll: select.poll = None
        log = logging.getLogger(__name__)
        self.log: logging.Logger = logging.LoggerAdapter(log, {"peer_name": server_name})
        self.log.setLevel(logging.DEBUG)
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
        except KeyboardInterrupt as e:
            self.log.info(f"\n{'-'*50}\nShutting down server")
        finally:
            self._cleanup()
            
    def _cleanup(self) -> None:
        """Shutdown server gracefully"""
        if self.epoll is not None:
            try:
                self.epoll.unregister(self.socket.fileno())
            except Exception:
                pass
            self.epoll.close()
            self.epoll = None
        self._close_server_socket()
        self.stop.set()
        for t in self.threads:
            t.join() 
    
    def _handle_events(self, timeout=1) -> None:
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

    def _recv_all(self, fileno: int)-> dict[str, any]:
        pass
        
    def _send_ack(self, payload: dict[str, str], fileno: int) -> None:
        pass

    def _parse_request(self, request: dict[str, str], fileno: int) -> dict[str, str]:
        pass

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