#!/usr/bin/env 

import os 
import sys
import time 
import json 
import socket 
import pickle 
import logging 
from urllib.request import urlopen 
from typing import Self, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(peer_name)s - %(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

class Client:
    BUFFER_SIZE = 2**10
    MAX_BACKOFF = 128 
    
    def __init__(self, client_name: str, host: str, port: int):
        self.client_name: str = client_name
        self.host: str = host 
        self.port: int = port 
        self.socket: socket.socket = None 
        self.backoff: int = 1 
        self.last_payload: bytes = None 
        log = logging.getLogger(__name__)
        self.log = logging.LoggerAdapter(log, {"peer_name": client_name})
        self.log.setLevel(logging.DEBUG)
        self._create_socket()

    def _create_socket(self) -> None:
        """Estbalish a connection to a specified (host, port) with exponential backoffs"""
        self._socket_close()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        while True:
            try:
                self.socket.settimeout(5.0)
                self.socket.connect((self.host, self.port))
                self.log.info(f"Connected to {self.host}:{self.port}")
                break 
            except Exception as e:
                self.log.error(f"_create_socket: {e}")
                self.log.info(
                    f"[RETRY]: Reconnection attempt in {self.backoff} seconds"
                )
                time.sleep(self.backoff)
                self.backoff = min(self.backoff * 2, self.MAX_BACKOFF)
                self._reset_raw_socket()

    def _reset_raw_socket(self) -> None:
        """Close and create new socket"""
        try:
            self.socket.close()
        except Exception as e:
            self.log.error(f"_reset_raw_socket: {e}")
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    def _socket_close(self) -> None:
        """Close connection established"""
        if self.socket is not None:
            try:
                self.socket.shutdown(socket.SHUT_RDWR)
            except Exception:
                pass
            finally:
                self.socket.close()
                self.socket = None

    def create():
        pass 
    
    def delete():
        pass
    
    def modify():
        pass

    def get_event():
        pass 
    
    def list_events():
        pass

    def search_events():
        pass

    def _connect_to_server():
        """Send msg to server and recieve acknowledgement"""
        pass 
    
    def _serialize_data():
        """serialize data to send over the wire"""
        pass 
    
    def _send_data():
        """Send payload to server"""
        pass 
    
    def _recv_ack():
        """Recieve acknowledgement from server"""
        pass 
    
    def _parse_ack():
        """Parse acknowledgement and return expected response"""
        pass 
                
    def __enter__(self) -> Self:
        return self 
    
    def __exit__(self, exception_type, excpetion_value, exception_traceback) -> None:
        self._socket_close()
        return

        
def main() -> None:
    if len(sys.argv) != 4:
        print(f"Usage: python {sys.argv[0]} <client_name> <host> <port>")
        sys.exit(1)

    client_name = sys.argv[1]
    host =  sys.argv[2]
    port = int(sys.argv[3])
    with Client(client_name=client_name, host=host, port=port) as client:
        print('wait')
        time.sleep(5)
        print('done')

if __name__ == "__main__":
    main()

        
        