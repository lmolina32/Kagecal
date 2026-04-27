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
        self.logical_clock: int = 0
        self.own_port: int = 0
        self.own_host: str = socket.gethostname()
        self.leaders_address: tuple[str, int] = ("", 0)
        self.log = logging.getLogger()
        self.pid = os.getpid()

        self.election_happening = False
        self.election_cv = threading.Condition()
        self.pause_server = threading.Event()
        self._server_thread: Optional[threading.Thread] = None

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

        self._bootstrap()

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
                self.server.leader_host, self.server.leader_port = (
                    client.who_is_leader()
                )
                self.server.set_mode(ServerMode.FOLLOWER)
                self.log.info(
                    f"Leader found at {self.server.leader_host}:{self.server.leader_port}"
                )
                break
            except TimeoutError:
                self.log.error(
                    f"Cannot reach peer {target_peer_ident} at {target_port}:{target_host}"
                )

        # Queried all Peers, got no response, trigger and election and win.
        else:
            # TODO: we talked about just calling the election, for ease of mind but wouldn't this just reiterate thorugh all peers again that it couldn't contact?
            self.call_election()

        # Got leader address syncronize with leader
        # TODO: should this be a loop, loop until leader is elected then make contact to sync. If we do loop, cover edge case were this peer becomes the leader
        try:
            client = Client(
                self.peer_ident,
                self.server.leader_host,
                self.server.leader_port,
                self.own_host,
                self.own_port,
            )
            # TODO: ensure when calling sync on the leader this is the tuple order returned
            calendar, logcial_clock = client.sync()
            self.server.update(calendar, logcial_clock)
        except TimeoutError:
            self.call_election()

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
                        # TODO: should we just keep open one socket connection for the leader?
                        client = Client(
                            self.peer_ident,
                            self.server.leader_host,
                            self.server.leader_port,
                            self.own_host,
                            self.own_port,
                        )
                        client.create(name, start, end, description, location, repeats)
                        event_id = self.server.persistence.create(
                            name, start, end, description, location, repeats
                        )
                    case ServerMode.LEADER:
                        event_id = self.server.persistence.create(
                            name, start, end, description, location, repeats
                        )
                        self.server.broadcast_clock()
        except TimeoutError:
            self.log.error("Leader unreachable during create(); calling election")
            self.call_election()
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
        except TimeoutError:
            self.log.error("Leader unreachable during delete(); calling election")
            self.call_election()

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
        except TimeoutError:
            self.log.error("Leader unreachable during modify(); calling election")
            self.call_election()
        return updated_id

    def _serve(self):
        """Target for the thread created by start_server."""
        while not self.pause_server.is_set():
            # 1. Serve a round of requests.
            flags = self.server.serve()

            # 2. Check for ELECTION
            if flags & ServerFlags.DO_ELECTION:
                # In this codepath, the server thread doesn't need to be stopped and restarted because it is internally calling the election.
                self.call_election()

            # 3. Check for SYNC
            if flags & ServerFlags.DO_SYNC:
                # Sync is a request to the leader, so it could fail, triggering an election.
                try:
                    client = Client(
                        self.peer_ident,
                        self.server.leader_host,
                        self.server.leader_port,
                        self.own_host,
                        self.own_port,
                    )
                    calendar, clock = client.sync()
                    self.server.update(calendar, clock)
                except TimeoutError:
                    self.log.error("Sync with leader failed, calling election")
                    self.call_election()

    # TODO: who ever calls an election is responsbile for create client socket for the leader
    def call_election(self):
        """Initiates (or continues) the leader election protocol. While this method is executing, this peer will stop serving incoming requests, and attempts to modify the calendar state will block, but local reads will not block. A new client to the new leader is created."""
        # TODO: Complete this method

        # 0. Set the election condition variable.
        with self.election_cv:
            self.election_happening = True

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
                break
        else:
            # b. If no OK received from any peer with higher PID (or if there are no peers with a higher PID), become the leader and send COORDINATE. Check the logical clock on all COORDINATE ACKs, and SYNC with highest one.
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
                if logical_clock > self.logical_clock:
                    clients.append((logical_clock, client))

            if clients:
                clients.sort(reverse=True)
                for clock, client in clients:
                    try:
                        events, logical_clock = client.sync()
                    except TimeoutError:
                        continue
                    self.server.update(events, logical_clock)
                    break

        # 3. Wake the UI thread if it is waiting for the election to finish to place write.
        with self.election_cv:
            self.election_happening = False
            self.election_cv.notify()

    def start_server(self):
        """Creates a background thread that serves incoming requests to the peer's server, and syncs the local calendar with the leader's calendar in the event of an update."""
        # TODO: Still need to add actual background thread at init
        if self._server_thread is not None and self._server_thread.is_alive():
            self.log.warning(
                "start_server() called when server thread is still running"
            )
            return

        self.pause_server.clear()
        self._server_thread = threading.Thread(target=self._serve, daemon=True)
        self._server_thread.start()
        self.log.info(f"Server thread start for peer {self.peer_ident}")

    def stop_server(self):
        """Stops a background thread that serves incoming requests to the peer's server, and syncs the local calendar with the leader's calendar in the event of an update."""
        self.pause_server.set()
        self._server_thread.join()
        # should this be a loop and check if we are leader or recieve sink to break out
        try:
            client = Client(
                self.peer_ident,
                self.server.leader_host,
                self.server.leader_port,
                self.own_host,
                self.own_port,
            )
            calendar, lock = client.sync()
            self.server.update(calendar, lock)
        except TimeoutError:
            self.call_election()
        self.log.info(f"server Thread stopped for peer {self.peer_ident}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage {sys.argv[0]} <calendar-project> <peer-name>", file=sys.stderr)
        sys.exit(1)

    peer = Peer(calendar_ident=sys.argv[1], peer_ident=sys.argv[2])
    peer.run()
