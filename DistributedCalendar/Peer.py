#!/usr/bin/env python3

import sys
import json
import random
import socket
import logging
from urllib.request import urlopen
from typing import List, Tuple, Dict

from .Server import Server
from .Client import Client


class Peer:
    def __init__(self) -> None:
        self.logical_clock: int = 0
        self.calendar_name: str = ""
        self.peer_name: str = ""
        self.own_port: int = 0

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

    def startup(self) -> None: ...

    def run(self) -> None: ...
