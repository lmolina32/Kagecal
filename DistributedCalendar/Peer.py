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
from .Calendar import Repeats, Event

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
        update: str = f"calendar_{self.calendar_ident}_{self.peer_ident}.update"

        time.sleep(time.time() % random.randint(1, 5))

        self.server: Server = Server(
            calendar_ident=self.calendar_ident,
            peer_ident=self.peer_ident,
            ckpt_path=ckpt,
            txn_path=txn,
            update_path=update,
            leader_host="",
            leader_port=0,
        )

        self.own_port = self.server.port
        self.own_host = self.server.host

        server_thread = threading.Thread(target=self._server_thread, daemon=True)
        election_thread = threading.Thread(target=self._election_thread, daemon=True)
        server_thread.start()
        election_thread.start()

        self._bootstrap()

    def _bootstrap(self) -> None:
        # TODO: add more detail docstring
        """Discover the existing peers and deterine the intial role of this node"""

        self.log.debug("Initialize queery for peers...")
        # peer_list = sorted(
        #     self._get_catalog(), key=lambda x: x["lastheardfrom"], reverse=True
        # )
        peer_list = self._get_catalog()
        random.shuffle(peer_list)
        self.log.debug(f"{self.peer_ident} found {len(peer_list)} peers")

        # Peer is the only one in the network, it becomes the leader
        if not peer_list:
            self.log.debug("No peers found, become leader")
            self.server.set_mode(ServerMode.LEADER)
            self.server.leader_host = self.own_host
            self.server.leader_port = self.own_port
            self.log.info(f"{self.peer_ident} is the leader")
            return

        # Query Peers for leaders address
        self.log.debug("peer list found starting to iterate")
        for peer_entry in peer_list:
            target_host, target_port, target_peer_ident = (
                peer_entry["host"],
                peer_entry["port"],
                peer_entry["peer_ident"],
            )

            if target_peer_ident == self.peer_ident:
                continue

            self.log.info(
                f"Attempting connection to {target_peer_ident} at {target_host}: {target_port}"
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
                self.log.info(
                    f" {target_peer_ident} says leader is at {target_host} {target_port}"
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
                except ConnectionError:
                    with self.client_cv:
                        self.client = None
                    continue

                with self.server.calendar_lock:
                    self.server.leader_host = leader_host
                    self.server.leader_port = leader_port

                self.server.set_mode(ServerMode.FOLLOWER)
                self.log.info(
                    f"Connected to leader {self.server.leader_host} {self.server.leader_port}."
                )
                break
            except ConnectionError:
                self.log.error(
                    f"Cannot reach peer {target_peer_ident} at {target_port}:{target_host}"
                )

        # Queried all Peers, got no response, trigger and election and win.
        else:
            self.log.info(f"Couldn't contact leader.")
            self.log.debug("callling an election")
            with self.election_cv:
                self.do_election = True
                self.election_cv.notify()

        # Syncronize with leader.
        self.log.debug("_bootstrap: syncing with the leader as followerb")
        if self.server.get_mode() == ServerMode.FOLLOWER:
            self.log.debug("_bootstrap: grabbing client cv")
            with self.client_cv:
                self.log.debug(f"_bootstrap: {self.client} will wait if not none")
                while not self.client:
                    self.client_cv.wait()
                try:
                    self.log.debug(f"_bootstrap: {self.client} syncing with peeer")
                    calendar, logical_clock = self.client.sync()
                    self.log.debug(f"_bootstrap: {self.client} updating calendar")
                    self.server.update(calendar, logical_clock)
                except ConnectionError:
                    self.log.debug(
                        f"_bootstrap: failed to sync with leader grabbing election_cv, and client_cv and calling an election"
                    )
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
    ) -> Optional[str]:
        """Grab the calendar lock first. If this peer is the leader, update the calendar directly then broadcast sync. Otherwise, use RPC stub on the client to send update to leader, then update local calendar to skip broadcast sync. On failure, raises ConnectionError."""
        match self.server.get_mode():
            case ServerMode.FOLLOWER:
                with self.client_cv:
                    while self.client is None:
                        self.client_cv.wait()
                    try:
                        with self.server.calendar_lock:
                            self.client.create(
                                name, start, end, description, location, repeats
                            )
                            event_id = self.server.persistence.create(
                                name, start, end, description, location, repeats
                            )
                    except ConnectionError:
                        self.log.error(
                            "Leader unreachable during create(); calling election"
                        )
                        with self.election_cv:
                            self.do_election = True
                            self.election_cv.notify()
                        raise ConnectionError("Failed to reach leader.")
            case ServerMode.LEADER:
                with self.server.calendar_lock:
                    event_id = self.server.persistence.create(
                        name, start, end, description, location, repeats
                    )
                self.server.broadcast_clock()
        return event_id

    def delete(self, ident: str) -> None:
        """Grab the calendar lock first. If this peer is the leader, update the calendar directly then broadcast sync. Otherwise, use RPC stub on the client to send update to leader, then update local calendar to skip broadcast sync. On failure, raises ConnectionError."""
        match self.server.get_mode():
            case ServerMode.FOLLOWER:
                with self.client_cv:
                    while self.client is None:
                        self.client_cv.wait()
                    try:
                        with self.server.calendar_lock:
                            self.client.delete(ident)
                            self.server.persistence.delete(ident)
                    except ConnectionError:
                        self.log.error(
                            "Leader unreachable during delete(); calling election"
                        )
                        with self.election_cv:
                            self.do_election = True
                            self.election_cv.notify()
                        raise ConnectionError("Failed to reach leader.")
            case ServerMode.LEADER:
                with self.server.calendar_lock:
                    self.server.persistence.delete(ident)
                self.server.broadcast_clock()

    def modify(
        self,
        ident: str,
        name: str,
        start: int,
        end: int,
        description: Optional[str],
        location: Optional[str],
        repeats: Optional[Repeats],
    ) -> Optional[str]:
        """Grab the calendar lock first. If this peer is the leader, update the calendar directly then broadcast sync. Otherwise, use RPC stub on the client to send update to leader, then update local calendar to skip broadcast sync. On failure, raises ConnectionError."""
        match self.server.get_mode():
            case ServerMode.FOLLOWER:
                with self.client_cv:
                    while self.client is None:
                        self.client_cv.wait()
                    try:
                        with self.server.calendar_lock:
                            self.client.modify(
                                ident, name, start, end, description, location, repeats
                            )
                            event_id = self.server.persistence.modify(
                                ident, name, start, end, description, location, repeats
                            )
                    except ConnectionError:
                        self.log.error(
                            "Leader unreachable during modify(); calling election"
                        )
                        with self.election_cv:
                            self.do_election = True
                            self.election_cv.notify()
                        raise ConnectionError("Failed to reach leader.")
            case ServerMode.LEADER:
                with self.server.calendar_lock:
                    event_id = self.server.persistence.modify(
                        ident, name, start, end, description, location, repeats
                    )
                self.server.broadcast_clock()

        return event_id

    def get_event(self, ident: str) -> Optional[Event]:
        """grab calendar lock and perform read"""
        with self.server.calendar_lock:
            event = self.server.persistence.get_event(ident)
        return event

    def list_events(self) -> dict[str, Event]:
        """grab calendar lock and perform read"""
        with self.server.calendar_lock:
            event = self.server.persistence.list_events()
        return event

    def _server_thread(self):
        """Target for the thread created by start_server."""
        while True:
            # 1. Serve a round of requests.
            flags = self.server.serve()
            self.log.debug(f"_server_thread: " f"flags for server {flags}")

            # 2. Check for ELECTION
            if flags & ServerFlags.DO_ELECTION:
                self.log.debug(f"_server_thread: " f"election called")
                # In this codepath, the server thread doesn't need to be stopped and restarted because it is internally calling the election.
                with self.election_cv:
                    self.do_election = True
                    self.election_cv.notify()

            # 3. Check for SYNC
            if flags & ServerFlags.DO_SYNC:
                self.log.debug(f"_server_thread: " f"synced called ")
                # Sync is a request to the leader, so it could fail, triggering an election.
                try:
                    with self.client_cv:
                        if self.client is None:
                            raise ConnectionError
                        calendar, clock = self.client.sync()
                    self.server.update(calendar, clock)
                except ConnectionError:
                    self.log.debug(
                        f"_server_thread: " f"synced failed calling an election"
                    )
                    with self.election_cv:
                        self.do_election = True
                        self.election_cv.notify()

            if flags & ServerFlags.NEW_LEADER:
                self.log.debug(f"_server_thread: " f"elect new leader")
                leader_host, leader_port = (
                    self.server.leader_host,
                    self.server.leader_port,
                )
                try:
                    self.log.debug(
                        f"_server_thread: "
                        f"setting new leader endpoint to {self.server.leader_host}:{self.server.leader_port}"
                    )
                    with self.client_cv:
                        self.client = Client(
                            self.peer_ident,
                            leader_host,
                            leader_port,
                            self.own_host,
                            self.own_port,
                        )
                        events, clock = self.client.sync()
                        self.client_cv.notify()
                    self.server.set_mode(ServerMode.FOLLOWER)
                    self.server.update(events, clock)
                    self.log.info(f"New leader ")
                except ConnectionError:
                    self.log.debug(
                        f"_server_thread: "
                        f" new leader endpoint failed, calling election"
                    )
                    with self.election_cv:
                        self.do_election = True
                        self.election_cv.notify()

    def call_election(self):
        """Initiates (or continues) the leader election protocol. A new client to the new leader is created."""
        self.log.info(f"Starting election. PID: {self.pid}")
        with self.client_cv:
            self.client = None

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
            self.log.info(
                f"Sending ELECT to {entry["peer_ident"]} with PID {entry["PID"]} at {entry["host"]} {entry["port"]}."
            )
            if client.call_election():
                # a. If OK received, wait for COORDINATE message on server forever. (In the event all peers die at this point and never recover, the application will have to be restarted by the user.)
                self.log.info(
                    f"{entry["peer_ident"]} at {entry["host"]} {entry["port"]} responded with OK."
                )
                self.server.set_coordinate(True)
                with self.client_cv:
                    while not self.client:
                        self.client_cv.wait()
                self.log.info("Received coordinate.")
                break
        else:
            # b. If no OK received from any peer with higher PID (or if there are no peers with a higher PID), become the leader and send COORDINATE. Check the logical clock on all COORDINATE ACKs, and SYNC with highest one.
            self.log.info(
                f"Won election. New leader endpoint is {self.own_host} {self.own_port}"
            )
            self.server.set_mode(ServerMode.LEADER)
            with self.client_cv:
                self.client = None

            clients = []
            for entry in lower_pids:
                try:
                    client = Client(
                        self.peer_ident,
                        entry["host"],
                        entry["port"],
                        self.own_host,
                        self.own_port,
                    )
                    logical_clock = client.coordinate()
                except ConnectionError:
                    continue
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
                while not self.do_election:
                    self.election_cv.wait()
                self.call_election()
                self.do_election = False


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage {sys.argv[0]} <calendar-project> <peer-name>", file=sys.stderr)
        sys.exit(1)

    peer = Peer(calendar_ident=sys.argv[1], peer_ident=sys.argv[2])
