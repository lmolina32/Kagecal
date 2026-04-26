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

    def __init__(
        self, target_host: str, target_port: int, own_host: str, own_port: int
    ):
        self.target_host: str = target_host
        self.target_port: int = target_port
        self.own_host: str = own_host
        self.own_port: int = own_port
        self.backoff: int = 1
        self.socket_backoff: int = 0
        self.log = logging.getLogger(__name__)
        self.log.setLevel(logging.DEBUG)

        self.socket: Optional[socket.socket] = None
        self._create_socket()

    def __del__(self):
        # TODO: Close the socket gracefully.
        pass

    def sync(self) -> tuple[list[Event], int]:
        """Retrieves the current calendar state and logical clock from the target peer. On success, returns a tuple containing the event list and logical clock. On failure, raises a TimeoutError."""

        pass

    def _create_socket(self) -> None:
        """Estbalish a connection to a specified (host, port) with exponential backoffs"""
        self._socket_close()
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            for _ in range(self.MAX_RETRIES):
                try:
                    self.socket.settimeout(5.0)
                    self.socket.connect((self.target_host, self.target_port))
                    self.log.info(f"Connected to {self.target_host}:{self.target_port}")
                    return
                except Exception as e:
                    self.log.error(f"_create_socket: {e}")
                    self.log.info(
                        f"[RETRY]: Reconnection attempt in {self.backoff} seconds"
                    )
                    time.sleep(self.backoff)
                    self.backoff = min(self.backoff * 2, self.MAX_BACKOFF)
                    self._reset_raw_socket()
        except KeyboardInterrupt as e:
            self.log.info("Client Shutting down")
            self._socket_close()
            return None
        self._socket_close()
        raise ConnectionError(f"Failed after {self.MAX_RETRIES} retries")

    def _reset_raw_socket(self) -> None:
        """Close and create new socket"""
        try:
            self._socket_close()
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

    def create(
        self,
        name: str,
        start: int,
        end: int,
        description: Optional[str] = None,
        location: Optional[str] = None,
        repeats: Optional[Repeats] = None,
    ) -> Optional[int]:

        send_msg = {
            "method": "create",
            "params": {
                "name": name,
                "start": start,
                "end": end,
                "description": description,
                "location": location,
                "repeats": repeats,
            },
            "from": (self.target_host, self.target_port),
        }
        self.log.info(send_msg)
        msg = self._serialize_data(send_msg)
        return self._connect_to_server(msg)

    def delete(self, ident: int) -> None:
        send_msg = {
            "method": "delete",
            "params": {"ident": ident},
            "from": (self.target_host, self.target_port),
        }
        msg = self._serialize_data(send_msg)
        return self._connect_to_server(msg)

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
        send_msg = {
            "method": "modify",
            "params": {
                "ident": ident,
                "name": name,
                "start": start,
                "end": end,
                "description": description,
                "location": location,
                "repeats": repeats,
            },
            "from": (self.host, self.target_port),
        }
        msg = self._serialize_data(send_msg)
        return self._connect_to_server(msg)

    def get_event(self, ident: int) -> Event:
        send_msg = {
            "method": "get_event",
            "params": {"ident": ident},
            "from": (self.host, self.target_port),
        }
        msg = self._serialize_data(send_msg)
        return self._connect_to_server(msg)

    def list_events(self) -> list[Event]:
        send_msg = {
            "method": "list_events",
            "from": (self.host, self.port),
        }
        msg = self._serialize_data(send_msg)
        return self._connect_to_server(msg)

    def who_is_leader(self) -> tuple[str, int]:
        send_msg = {
            "method": "who_is_leader",
            "from": (self.host, self.port),
        }
        msg = self._serialize_data(send_msg)
        return self._connect_to_server(msg)

    def call_election(self) -> bool:
        """Send an ELECTION message to the target peer. Returns True if received OK from target, or False otherwise."""
        # TODO: IMplement method
        pass

    def coordinate(self) -> int:
        """Send a COORDINATE message to the target peer. Return the logical clock from the response, or 0 if the request fails."""
        # TODO: Implement method
        pass

    def register_and_sync(self, host: str, port: int) -> tuple[int, dict[int, Event]]:
        send_msg = {
            "method": "register_and_sync",
            "params": {"host": host, "port": port},
            "from": (self.host, self.port),
        }
        msg = self._serialize_data(send_msg)
        return self._connect_to_server(msg)

    def _connect_to_server(self, payload: bytes) -> bool | int | None:
        """Send msg to server and recieve acknowledgement"""
        try:
            if not self.socket:
                self._create_socket()
            for _ in range(self.MAX_RETRIES):
                try:
                    self._send_data(payload)
                    return self._recv_ack()
                except Exception as e:
                    self.log.error(f"connect_to_server: {e}")
                    self.log.info(
                        f"[RETRY]: Reconnection attempt in {self.backoff} seconds"
                    )
                    time.sleep(self.backoff)
                    self.backoff = min(self.backoff * 2, self.MAX_BACKOFF)
                    self._create_socket()
        except KeyboardInterrupt as e:
            self.log.info("Client Shutting down")
            self._socket_close()
            return None
        self._socket_close()
        raise ConnectionError(f"Failed after {self.MAX_RETRIES} retries")

    def _serialize_data(self, payload: dict[str, str | int | Repeats]) -> bytes:
        """serialize data to send over the wire"""
        pickled_msg = pickle.dumps(payload)
        header = str(len(pickled_msg)).encode() + b"\n"
        msg = header + pickled_msg
        return msg

    def _send_data(self, payload: bytes) -> None:
        """Send payload to server"""
        if self.socket is None:
            self._create_socket()
        assert self.socket is not None
        self.socket.settimeout(5.0)
        self.socket.sendall(payload)

    def _recv_ack(self) -> bool | int | None:
        """Recieve acknowledgement from server"""
        header = b""
        assert self.socket is not None

        # 1. Recieve payload header (length)
        self.socket.settimeout(5.0)
        while b"\n" not in header:
            data = self.socket.recv(self.BUFFER_SIZE)
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

        if len(buffer) == 0:
            raise Exception("Buffer read in 0 bytes")
        payload = pickle.loads(buffer)
        return self.parse_ack(payload)

    def parse_ack(self, payload: dict[str, str | bytes | bool]) -> bool | int | None:
        """Parse acknowledgement and return expected response"""
        status = payload.get("status", "")
        if status == "success":
            self.log.info(
                f"Success: Received Payload for {payload.get('method', '')} {payload.get('key', '')}"
            )
            method = payload.get("method", "")

            match method:
                case "create":
                    return payload.get("ident", None)
                case "delete":
                    return True
                case "modify":
                    return payload.get("ident", None)
                case "get_event":
                    return payload.get("event", None)
                case "list_events":
                    return payload.get("calendar", {})
                case "who_is_leader":
                    return payload.get("host", ""), payload.get("port", 0)
                case "register_and_sync":
                    return payload.get("logical_clock", 0), payload.get("calendar", {})
                case _:
                    raise Exception(f"Unknown method in ACK: {method}")
        elif status == "failure":
            raise Exception(f"Error: {payload.get('error', 'unknown error')}")
        else:
            raise Exception(f"Malformed ACK: missing status")
        return None

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
    host = sys.argv[2]
    port = int(sys.argv[3])
    with Client(
        client_name=client_name,
        host=host,
        port=port,
        own_host=socket.gethostname(),
        own_port=0,
    ) as client:
        now_utc = int(datetime.now(timezone.utc).timestamp())
        one_hour_later = int(
            (datetime.now(timezone.utc) + timedelta(hours=1)).timestamp()
        )
        for i in range(25):
            id = client.create(
                f"progress report{i}",
                start=now_utc,
                end=one_hour_later,
                location="",
                description="",
            )
            client.delete(id)
            id = client.create(
                f"progress report{i}",
                start=now_utc,
                end=one_hour_later,
                location="",
                description="",
            )
            client.modify(
                id,
                f"progress report{i}",
                start=now_utc,
                end=one_hour_later,
                location=str(i),
                description=str(i),
            )


if __name__ == "__main__":
    main()
