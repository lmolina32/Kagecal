#!/usr/bin/env python3

import sys
import time
import socket
import pickle
import logging
from datetime import datetime, timezone, timedelta
from typing import Self, Optional

from .Calendar import Repeats, Event

logging.basicConfig(
    level=logging.INFO,
    format="[%(levelname)s %(asctime)s %(module)s:%(lineno)d] %(message)s",
    datefmt="%H:%M:%S",
)

Socket = socket.socket


class Client:
    BUFFER_SIZE = 1 << 12
    MAX_BACKOFF = 128
    MAX_RETRIES = 4
    ELECTION_MAX_RETRIES = 3
    SOCKET_TIMEOUT = 10  # Seconds

    def __init__(
        self,
        peer_ident: str,
        target_host: str,
        target_port: int,
        own_host: str,
        own_port: int,
    ):
        self.peer_ident = peer_ident
        self.target_host: str = target_host
        self.target_port: int = target_port
        self.own_host: str = own_host
        self.own_port: int = own_port

        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.DEBUG)

        self.socket: Optional[socket.socket] = None
        self._create_socket()

    def __del__(self):
        self._socket_close()

    # === RPC STUBS ===
    def create(
        self,
        name: str,
        start: int,
        end: int,
        description: Optional[str] = None,
        location: Optional[str] = None,
        repeats: Optional[Repeats] = None,
    ) -> Optional[int]:
        """RPC stub that creates an Event, assigns it a unique identifier, and adds it to the calendar. If the event metadata is malformed, does nothing. Returns the identifer for the event. On failure, raises a ConnectionError."""
        message = {
            "method": "create",
            "peer_ident": self.peer_ident,
            "params": {
                "name": name,
                "start": start,
                "end": end,
                "description": description,
                "location": location,
                "repeats": repeats,
            },
        }
        response = self._send(message)
        if response["status"] == "success":
            return response["ident"]
        else:
            return None

    def delete(self, ident: int) -> None:
        """RPC stub that Deletes an event with a given identifier from the calendar, regardless of whether or not the event exists. On failure, raises a ConnectionError."""
        message = {
            "method": "delete",
            "peer_ident": self.peer_ident,
            "params": {"ident": ident},
        }
        response = self._send(message)

    def modify(
        self,
        ident: int,
        name: str,
        start: int,
        end: int,
        description: Optional[str] = None,
        location: Optional[str] = None,
        repeats: Optional[Repeats] = None,
    ) -> Optional[int]:
        message = {
            "method": "modify",
            "peer_ident": self.peer_ident,
            "params": {
                "ident": ident,
                "name": name,
                "start": start,
                "end": end,
                "description": description,
                "location": location,
                "repeats": repeats,
            },
        }
        response = self._send(message)
        if response["status"] == "success":
            return response["ident"]
        else:
            return None

    def coordinate(self) -> int:
        """Send a COORDINATE message to the target peer. Return the logical clock from the response, or 0 if the request fails."""
        message = {
            "method": "coordinate",
            "peer_ident": self.peer_ident,
            "params": {"host": self.own_host, "port": self.own_port},
        }
        try:
            response = self._send(message)
        except ConnectionError:
            return 0
        return response["logical_clock"]

    def who_is_leader(self) -> tuple[str, int]:
        message = {
            "method": "who_is_leader",
            "peer_ident": self.peer_ident,
            "params": {},
        }
        response = self._send(message)
        return response["host"], response["port"]

    def call_election(self) -> bool:
        """Send an ELECTION message to the target peer. Returns True if received OK from target, or False otherwise."""
        message = {
            "method": "election",
            "peer_ident": self.peer_ident,
            "params": {},
        }
        try:
            self._send(message)
            return True
        except ConnectionError:
            return False

    def sync(self) -> tuple[dict[int, Event], int]:
        """Retrieves the current calendar state and logical clock from the target peer. On success, returns a tuple containing the event list and logical clock. On failure, raises a TimeoutError."""
        message = {
            "method": "who_is_leader",
            "peer_ident": self.peer_ident,
            "params": {},
        }
        response = self._send(message)
        return response["host"], response["port"]

    # === SOCKETS ===
    def _send(self, message: dict) -> dict:
        """Sends a message to the client, validates the response, and returns it. On failure, raises a ConnectionError"""
        # 1. SEND MESSAGE
        # Serialize message to bytes
        pickled_msg = pickle.dumps(message)
        header = str(len(pickled_msg)).encode() + b"\n"
        message_bytes = header + pickled_msg

        # Send message to server
        if self.socket is None:
            self._create_socket()

        try:
            self.socket.sendall(message_bytes)
        except Exception:
            raise ConnectionError("Failed to send RPC to server.")

        # 2. RECEIVE RESPONSE
        header = b""
        # 1. Recieve payload header (length)
        while b"\n" not in header:
            try:
                data = self.socket.recv(self.BUFFER_SIZE)
            except Exception:
                raise ConnectionError("Failed to recv response from server.")

            if not data:
                raise ConnectionError("Connection closed from server try again")
            header += data

        delim_idx = header.index(b"\n")
        data_size = int(header[:delim_idx].decode())
        buffer = header[delim_idx + 1 :]
        read_amt = len(buffer)

        # 2. Receive payload of specified size
        while read_amt < data_size:
            remaining = data_size - read_amt
            data = self.socket.recv(min(self.BUFFER_SIZE, remaining))
            if not data:
                raise ConnectionError("Connection closed mid-payload")
            read_amt += len(data)
            buffer += data

        try:
            payload = pickle.loads(buffer)
        except pickle.UnpicklingError:
            raise ConnectionError("Invalid RPC.")

        return payload

    def _create_socket(self) -> None:
        """Open a socket to the client's target host and port using exponential backoff. Raises ConnectionError if max retries exceeded."""
        self._socket_close()
        backoff = 1
        for _ in range(self.MAX_RETRIES):
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(self.SOCKET_TIMEOUT)
            try:
                self.socket.connect((self.target_host, self.target_port))
                self.log.info(f"Connected to {self.target_host}:{self.target_port}")
                return
            except Exception as e:
                self.log.info(f"[RETRY]: Reconnection attempt in {backoff} seconds")
                time.sleep(backoff)
                backoff = min(backoff * 2, self.MAX_BACKOFF)
                try:
                    self._socket_close()
                except Exception as e:
                    self.log.error(f"_reset_raw_socket: {e}")
                self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._socket_close()
        raise ConnectionError(f"Failed after {self.MAX_RETRIES} retries")

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

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exception_type, excpetion_value, exception_traceback) -> None:
        self._socket_close()


def main() -> None:
    if len(sys.argv) != 4:
        print(f"Usage: python {sys.argv[0]} <peer_ident> <target_host> <target_port>")
        sys.exit(1)

    peer_ident = sys.argv[1]
    target_host = sys.argv[2]
    target_port = int(sys.argv[3])
    with Client(
        peer_ident,
        target_host,
        target_port,
        socket.gethostname(),
        0,
    ) as client:
        now_utc = int(datetime.now(timezone.utc).timestamp())
        one_hour_later = int(
            (datetime.now(timezone.utc) + timedelta(hours=1)).timestamp()
        )
        id = client.create(
            f"progress report",
            start=now_utc,
            end=one_hour_later,
            location="",
            description="",
        )
        client.delete(id)
        id = client.create(
            f"progress report",
            start=now_utc,
            end=one_hour_later,
            location="",
            description="",
        )
        client.modify(
            id,
            f"progress report",
            start=now_utc,
            end=one_hour_later,
            location="fitz",
            description="fitz",
        )


if __name__ == "__main__":
    main()
