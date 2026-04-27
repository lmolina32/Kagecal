#!/usr/bin/env python3
import os
import sys
import time
import json
import random
import socket
import logging
import threading
from urllib.request import urlopen
from typing import TypedDict, Optional

from .Server import Server, ServerMode, ServerFlags
from .Client import Client
from .PersistantCalendar import PersistantCalendar
from .Calendar import Repeats

log_format = "[%(levelname)s %(asctime)s %(module)s:%(lineno)d] %(message)s"
logging.basicConfig(
    format=log_format,
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO,
)


class CatalogEntry(TypedDict):
    owner: str
    lastheardfrom: int
    project: str
    calendar_ident: str
    peer_ident: str
    port: int
    host: str
    PID: int


class Peer:
    def __init__(
        self,
        calendar_ident: str,
        peer_ident: str,
    ) -> None:
        self.calendar_ident: str = calendar_ident
        self.peer_ident: str = peer_ident

        self.log = logging.getLogger()
        self.pid = os.getpid()

        # This CV is set by the server and client threads and waited on by the election thread. Set this True when an election needs to happen.
        self.do_election = False
        self.election_cv = threading.Condition()

        # This CV is set by the server and waitedon by the client thread. Set client to None when an election needs to happen, and wait on client to be set before doing any client operations.
        self.client = None
        self.client_cv = threading.Condition()

        ckpt: str = f"calendar_{self.calendar_ident}_{self.peer_ident}.ckpt"
        txn: str = f"calendar_{self.calendar_ident}_{self.peer_ident}.txn"

        time.sleep(time.time() % random.randint(1, 5))

        self.server: Server = Server(
            calendar_ident=self.calendar_ident,
            peer_ident=self.peer_ident,
            ckpt_path=ckpt,
            txn_path=txn,
            leader_host="",
            leader_port=0,
        )

        self.own_port = self.server.port
        self.own_host = self.server.host
        self._bootstrap()

        threading.Thread(target=self._server_thread(), daemon=True)
        threading.Thread(target=self._election_thread(), daemon=True)

    def _bootstrap(self) -> None:
        # TODO: add more detail docstring
        """Discover the existing peers and deterine the intial role of this node"""

        peer_list = sorted(
            self._get_catalog(), key=lambda x: x["lastheardfrom"], reverse=True
        )
        self.log.debug(f"{self.peer_ident} found {len(peer_list)} peers")

        # Peer is the only one in the network, it becomes the leader
        if not peer_list:
            self.server.set_mode(ServerMode.LEADER)
            self.server.leader_host = self.own_host
            self.server.leader_port = self.own_port
            self.log.info(f"{self.peer_ident} is the leader")
            return

        # Query Peers for leaders address
        for peer_entry in peer_list:
            target_host, target_port, target_peer_ident = (
                peer_entry["host"],
                peer_entry["port"],
                peer_entry["peer_ident"],
            )

            if target_peer_ident == self.peer_ident:
                continue

            self.log.debug(
                f"Attempting connection to {target_peer_ident} at {target_host}:{target_port}"
            )

            # Attempt to make connection, if succesful update leaders host & port, then set peer to follower
            try:
                client = Client(
                    self.peer_ident,
                    target_host,
                    target_port,
                    self.own_host,
                    self.own_port,
                )
                # TODO: check if this is the current output of who is leader
                leader_host, leader_port = client.who_is_leader()
                self.server.set_mode(ServerMode.FOLLOWER)

                try:
                    with self.client_cv:
                        self.client = Client(
                            self.peer_ident,
                            leader_host,
                            leader_port,
                            self.own_host,
                            self.own_port,
                        )
                except ConnectionError:
                    continue

                self.server.leader_host = leader_host
                self.server.leader_port = leader_port

                self.log.info(
                    f"Leader found at {self.server.leader_host}:{self.server.leader_port}"
                )
                break
            except ConnectionError:
                self.log.error(
                    f"Cannot reach peer {target_peer_ident} at {target_port}:{target_host}"
                )

        # Queried all Peers, got no response, trigger and election and win.
        else:
            self.log.info(f"Couldn't contact leader.")
            with self.election_cv:
                self.log.info(f"[ Main ] Taken election CV.")
                self.do_election = True
                self.election_cv.notify()

        # Syncronize with leader.
        if self.server.mode == ServerMode.FOLLOWER:
            with self.client_cv:
                while not self.client:
                    self.client_cv.wait()
                try:
                    calendar, logical_clock = self.client.sync()
                    self.server.update(calendar, logical_clock)
                except ConnectionError:
                    with self.election_cv, self.client_cv:
                        self.client = None
                        self.do_election = True
                        self.election_cv.notify()

    def _get_catalog(self) -> list[CatalogEntry]:
        """Retrieves all peers registered to the ND catalog server for this calendar."""
        nd_catalog: str = "http://catalog.cse.nd.edu:9097/query.json"
        try:
            with urlopen(nd_catalog) as res:
                raw_data = res.read()
                json_data = json.loads(raw_data)
        except Exception:
            # TODO: Maybe this should retry forever. We certainly will run into split brain if we let this go.
            self.log.critical("Failed to retrieve catalog.")
            return []

        peers = []
        for entry in json_data:
            if (
                entry.get("project", None) == "kagecal"
                and entry.get("calendar_ident", None) == self.calendar_ident
            ):
                host, port = entry.get("host"), entry.get("port")
                if host == self.own_host and port == self.own_port:
                    continue
                peers.append(entry)

        return peers

    # TODO: Write all the calendar stubs, make the mutating ones wait on the election condition variable.
    # TODO: reads automatically happen in the kagecal, should we lock?
    # TODO: figure out what will be passed down from the UI thread into RPC methods for peer
    def create(
        self,
        name: str,
        start: int,
        end: int,
        description: Optional[str],
        location: Optional[str],
        repeats: Optional[Repeats],
    ) -> Optional[int]:
        """Grab the calendar lock first. If this peer is the leader, update the calendar directly then broadcast sync. Otherwise, use RPC stub on the client to send update to leader, then update local calendar to skip broadcast sync. If RPC stub on client fails catch the exception and raise an election"""
        try:
            with self.server.calendar_lock:
                match self.server.mode:
                    case ServerMode.FOLLOWER:
                        self.client.create(
                            name, start, end, description, location, repeats
                        )
                        event_id = self.server.persistence.create(
                            name, start, end, description, location, repeats
                        )
                    case ServerMode.LEADER:
                        event_id = self.server.persistence.create(
                            name, start, end, description, location, repeats
                        )
                        self.server.broadcast_clock()
        except ConnectionError:
            self.log.error("Leader unreachable during create(); calling election")
            with self.election_cv, self.client_cv:
                self.client = None
                self.do_election = True
                self.election_cv.notify()
        return event_id

    def delete(self, ident: int) -> None:
        """Grab the calendar lock first. If this peer is the leader, update the calendar directly then broadcast sync. Otherwise, use RPC stub on the client to send update to leader, then update local calendar to skip broadcast sync. If RPC stub on client fails catch the exception and raise an election"""
        try:
            with self.server.calendar_lock:
                match self.server.mode:
                    case ServerMode.FOLLOWER:
                        client = Client(
                            self.peer_ident,
                            self.server.leader_host,
                            self.server.leader_port,
                            self.own_host,
                            self.own_port,
                        )
                        client.delete(ident)
                        self.server.persistence.delete(ident)
                    case ServerMode.LEADER:
                        self.server.persistence.delete(ident)
                        self.server.broadcast_clock()
        except ConnectionError:
            self.log.error("Leader unreachable during delete(); calling election")
            with self.election_cv, self.client_cv:
                self.client = None
                self.do_election = True
                self.election_cv.notify()

    def modify(
        self,
        ident: int,
        name: str,
        start: int,
        end: int,
        description: Optional[str],
        location: Optional[str],
        repeats: Optional[Repeats],
    ) -> Optional[int]:
        """Grab the calendar lock first. If this peer is the leader, update the calendar directly then broadcast sync. Otherwise, use RPC stub on the client to send update to leader, then update local calendar to skip broadcast sync. If RPC stub on client fails catch the exception and raise an election"""
        try:
            with self.server.calendar_lock:
                match self.server.mode:
                    case ServerMode.FOLLOWER:
                        client = Client(
                            self.peer_ident,
                            self.server.leader_host,
                            self.server.leader_port,
                            self.own_host,
                            self.own_port,
                        )
                        client.modify(
                            ident, name, start, end, description, location, repeats
                        )
                        updated_id = self.server.persistence.modify(
                            ident, name, start, end, description, location, repeats
                        )
                    case ServerMode.LEADER:
                        updated_id = self.server.persistence.modify(
                            ident, name, start, end, description, location, repeats
                        )
                        self.server.broadcast_clock()
        except ConnectionError:
            self.log.error("Leader unreachable during modify(); calling election")
            with self.election_cv, self.client_cv:
                self.client = None
                self.do_election = True
                self.election_cv.notify()
        return updated_id

    def _server_thread(self):
        """Target for the thread created by start_server."""
        while True:
            # 1. Serve a round of requests.
            flags = self.server.serve()

            # 2. Check for ELECTION
            if flags & ServerFlags.DO_ELECTION:
                # In this codepath, the server thread doesn't need to be stopped and restarted because it is internally calling the election.
                with self.election_cv:
                    self.do_election = True
                    self.election_cv.notify()

            # 3. Check for SYNC
            if flags & ServerFlags.DO_SYNC:
                # Sync is a request to the leader, so it could fail, triggering an election.
                try:
                    calendar, clock = self.client.sync()
                    self.server.update(calendar, clock)
                except ConnectionError:
                    with self.election_cv, self.client_cv:
                        self.client = None
                        self.do_election = True
                        self.election_cv.notify()

            if flags & ServerFlags.NEW_LEADER:
                leader_host, leader_port = (
                    self.server.leader_host,
                    self.server.leader_port,
                )
                try:
                    with self.client_cv:
                        self.client = Client(
                            self.peer_ident,
                            leader_host,
                            leader_port,
                            self.own_host,
                            self.own_port,
                        )
                        events, clock = self.client.sync()
                        self.server.update(events, clock)
                        self.new_leader = True
                        self.client_cv.notify()
                except ConnectionError:
                    with self.election_cv, self.client_cv:
                        self.client = None
                        self.do_election = True
                        self.election_cv.notify()

    def call_election(self):
        """Initiates (or continues) the leader election protocol. A new client to the new leader is created."""

        # 1. Get all peer endpoints from catalog.
        catalog_entries = self._get_catalog()
        lower_pids = []
        higher_pids = []

        # TODO: Tiebreaker if PIDs are the same.
        for entry in catalog_entries:
            if entry["PID"] > self.pid:
                higher_pids.append(entry)
            else:
                lower_pids.append(entry)

        # 2. From highest PID to lowest PID, send an ELECTION message to the peer.
        higher_pids.sort(key=lambda x: x["lastheardfrom"], reverse=True)
        higher_pids.sort(key=lambda x: x["PID"], reverse=True)
        for entry in higher_pids:
            client = Client(
                self.peer_ident,
                entry["host"],
                entry["port"],
                self.own_host,
                self.own_port,
            )
            if client.call_election():
                # a. If OK received, wait for COORDINATE message on server forever. (In the event all peers die at this point and never recover, the application will have to be restarted by the user.)
                self.server.set_coordinate(True)
                with self.client_cv:
                    while not self.client:
                        self.client_cv.wait()
                self.log.info("Received coordinate.")
                break
        else:
            # b. If no OK received from any peer with higher PID (or if there are no peers with a higher PID), become the leader and send COORDINATE. Check the logical clock on all COORDINATE ACKs, and SYNC with highest one.
            self.log.info("Won election.")
            self.server.set_mode(ServerMode.LEADER)

            clients = []
            for entry in lower_pids:
                client = Client(
                    self.peer_ident,
                    entry["host"],
                    entry["port"],
                    self.own_host,
                    self.own_port,
                )
                logical_clock = client.coordinate()
                if logical_clock > self.server.get_logical_clock():
                    clients.append((logical_clock, client))

            if clients:
                clients.sort(reverse=True)
                for clock, client in clients:
                    try:
                        events, logical_clock = client.sync()
                    except ConnectionError:
                        continue
                    self.server.update(events, logical_clock)
                    break

    def _election_thread(self):
        """Target for a thread that waits for the server or main threads to signal that election needs to happen."""
        while True:
            with self.election_cv:
                self.log.info(f"[ Election ] Taken election CV.")
                while not self.do_election:
                    self.election_cv.wait()
                self.log.info("Calling an election...")
                self.call_election()
                self.do_election = False


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage {sys.argv[0]} <calendar-project> <peer-name>", file=sys.stderr)
        sys.exit(1)

    peer = Peer(calendar_ident=sys.argv[1], peer_ident=sys.argv[2])
