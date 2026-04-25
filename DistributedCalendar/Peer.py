#!/usr/bin/env python3

import sys
import time
import json
import random
import socket
import logging
from urllib.request import urlopen
from typing import List, Tuple, Dict

from .Server import Server, ServerMode
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
        self.own_host: str = socket.gethostname()
        self.leaders_address: Tuple[str, int] = ("", 0)
        self.log = logging.getLogger()
        self.server: Server = self.startup()
        # TODO: UUID for pid, save in ckpt log, timestamp?

    def discovery_peers(self) -> List[Tuple[str, int, str]]:
        """
        Retrieve all Peers registered to the ND catalog server for this Calendar Project

        Returns:
            List of Tuples containing peers host, port, and name
        """
        nd_catalog: str = "http://catalog.cse.nd.edu:9097/query.json"
        try:
            with urlopen(nd_catalog) as res:
                raw_data = res.read()
                json_data = json.loads(raw_data)
        except Exception:
            return []

        peers = []
        for entry in json_data:
            if (
                entry.get("type", "") == "calendar"
                # TODO: low priority -> add option to set the user to be the owner of a calendar, and then not check this, e.g Sam is owner of this peer, Leo is owner of this peer.
                and entry.get("owner") == "lmolina3"
                and entry.get("project") == self.calendar_name
            ):
                host, port = entry.get("host"), entry.get("port")
                if host == self.own_host and port == self.own_port:
                    continue
                peers.append(
                    (
                        host,
                        port,
                        entry.get("lastheardfrom", 0),
                        entry.get("peer_name", ""),
                    )
                )
        # Sort by most recent timestamp
        peers.sort(key=lambda x: x[2], reverse=True)
        return [(h, p, n) for h, p, _, n in peers]

    def sync_with_leader(self) -> int: ...

    def startup(self) -> Server:
        """
        Starts the Peer server component. Two paths could occur:
        - The peer becomes the leader, all traffic will flow through it
        - The peer becomes a follower, will contact the leader for all CRUD operations

        Returns:
            Server: An instance of the peers server
        """
        # TODO: Need to make more robust
        ckpt: str = f"calendar_{self.calendar_name}_{self.peer_name}.ckpt"
        txn: str = f"calendar_{self.calendar_name}_{self.peer_name}.txn"

        time.sleep(time.time() % random.randint(1, 5))

        server = Server(
            calendar_ident=self.calendar_name,
            peer_ident=self.peer_name,
            ckpt_path=ckpt,
            txn_path=txn,
        )

        self.own_host = server.host
        self.own_port = server.port

        peer_list = self.discovery_peers()
        self.log.info(f"{self.peer_name} found {len(peer_list)} peers")
        # Staring off the network
        if not peer_list:
            server.mode = ServerMode.LEADER
            server.leaders_address = (self.own_host, self.own_port)
            self.log.info(f"{self.peer_name} is the leader")
            return server

        # Query Peers for leaders address
        for host, port, name in peer_list:
            self.log.info(f"Attempting connection with {name}, {host}:{port}")
            try:
                with Client(
                    client_name=self.peer_name,
                    host=host,
                    port=port,
                    own_host=self.own_host,
                    own_port=self.own_port,
                ) as client:
                    # TODO: replacing entire calendar state, could be done another way doing a diff of the calendar
                    leader_host, leader_port = client.who_is_leader()
                    server.leaders_address = (leader_host, leader_port)
                    self.log.info(f"Found leaders address: {leader_host}:{leader_port}")
                    break
            # TODO: maybe have costume errors.
            except Exception as e:
                self.log.info(f"Cannot reach peer: {name}, {host}:{port}")
                pass
        if server.leaders_address == ("", 0):
            # should trigger election here
            self.log.error(
                "CURRENTLY SHOULD NOT TRIGGER OUR CODE IS CHOPPED AND CHUDDED"
            )
            return server

        # Get leader address, if not responding start election
        try:
            with Client(
                client_name=self.peer_name,
                host=server.leaders_address[0],
                port=server.leaders_address[1],
                own_host=self.own_host,
                own_port=self.own_port,
            ) as client:
                # current idea: have leader return logical clock, leader calendar, set the logical clock + leaders calendar
                # idea -> do i expose the persistence calendar a level up, e.g declare it on this module, then pass it down to the server, e.g have the same object in memory but can operate on it without doing self.sever.persistence.
                logical_clock, leaders_calendar = client.register_and_sync(
                    self.own_host, self.own_port
                )
                # yuck
                server.persistence.calendar.events = leaders_calendar
                server.persistence._logical_clock = logical_clock
                server.mode = ServerMode.FOLLOWER
                self.log.info(
                    f"Fully synced with leader {len(server.persistence.calendar.events)}"
                )
        except Exception as e:
            # TODO: Start election
            pass

        return server

    def run(self) -> None:
        try:
            while True:
                self.server.serve()
                # TODO: add logic for when not polling
        except KeyboardInterrupt:
            self.log.info(f"{'-'*50}\nClosing down Peer")
        finally:
            ...
            # self.server._cleanup()

    def send_request(self) -> None:
        # TODO: this theoretically will be called by the CLI, determine correct args
        # TODO: have logic if you are the leader (need a resync)
        # TODO: have logic if you are the follower (send to leader)
        if self.server.mode == ServerMode.LEADER:
            ...
            # TODO: should we let the peer add the calender then resync with others
            # Idea: add to calendar, then have a background thread send to all peers in the system.
            return

        # TODO: follower logic, use Client to send request to leader
        ...


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage {sys.argv[0]} <calendar-project> <peer-name>", file=sys.stderr)
        sys.exit(1)

    peer = Peer(calendar_name=sys.argv[1], peer_name=sys.argv[2])
    peer.run()
