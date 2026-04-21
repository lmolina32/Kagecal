#!/usr/bin/env python3

import sys
import time
import json
import random
import socket
import logging
from urllib.request import urlopen
from typing import List, Tuple, Dict

from .Server import Server
from .Client import Client

log_format = "[%(levelname)s %(asctime)s %(module)s:%(lineno)d] %(message)s"
logging.basicConfig(
    format=log_format,
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)


class Peer:
    def __init__(self, calendar_name: str, peer_name: str) -> None:
        self.calendar_name: str = calendar_name
        self.peer_name: str = peer_name
        self.logical_clock: int = 0
        self.own_port: int = 0

        self.log = logging.getLogger()
        self.server: Server = self.startup()

    def discovery_peers(self) -> List[Tuple[str, int, str]]:
        """
        Retrieve all Peers registered to the ND catalog server for this Calendar Project

        Returns:
            List of Tuples containing peers host, port, and name
        """
        nd_catalog: str = "http://catalog.cse.nd.edu:9097//query.json"
        try:
            with urlopen(nd_catalog) as res:
                raw_data = res.read()
                json_data = json.loads(raw_data)
        except Exception:
            return []

        own_hostname = socket.gethostname()
        peers = []
        for entry in json_data:
            if (
                entry.get("type", "") == "calendar"
                and entry.get("owner") == "lmolina3"
                and entry.get("project") == self.peer_name
            ):
                host, port = entry.get("name"), entry.get("port")
                if host == own_hostname and port == self.own_port:
                    continue
                peers.append(
                    (
                        host,
                        port,
                        entry.get("lastheardfrom", 0),
                        entry.get("peer_name", ""),
                    )
                )

        peers.sort(key=lambda x: x[2], reverse=True)
        return [(h, p, n) for h, p, _, n in peers]

    def sync_with_leader(self) -> int: ...

    def startup(self) -> Server:
        # TODO: Need to make more robust
        ckpt: str = f"calendar_{self.calendar_name}_{self.peer_name}.ckpt"
        txn: str = f"calendar_{self.calendar_name}_{self.peer_name}.txn"

        time.sleep(time.time() % random.randint(1, 5))

        server = Server(
            project_name=self.calendar_name,
            server_name=self.peer_name,
            ckpt_path=ckpt,
            txn_path=txn,
        )

        server.start()
        self.own_port = server.port

        # TODO: ADD logic to find initial peer, leader or not leader

        return server

    def run(self) -> None:
        try:
            while True:
                self.server._handle_events(timeout=1)
                # TODO: add logic for when not polling
        except KeyboardInterrupt:
            self.log.info(f"{'-'*50}\nClosing down Peer")
        finally:
            self.server._cleanup()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage {sys.argv[0]} <calendar-project> <peer-name>", file=sys.stderr)
        sys.exit(1)

    peer = Peer(calendar_name=sys.argv[1], peer_name=sys.argv[2])
    peer.run()
