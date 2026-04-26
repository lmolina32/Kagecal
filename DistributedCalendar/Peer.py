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
from typing import TypedDict

from .Server import Server, ServerMode, ServerFlags
from .Client import Client

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
    def __init__(self, calendar_ident: str, peer_ident: str) -> None:
        self.calendar_ident: str = calendar_ident
        self.peer_ident: str = peer_ident
        self.logical_clock: int = 0
        self.own_port: int = 0
        self.own_host: str = socket.gethostname()
        self.leaders_address: Tuple[str, int] = ("", 0)
        self.log = logging.getLogger()
        self.server: Server = self.startup()
        self.pid = os.getpid()

        self.election_happening = False
        self.election_cv = threading.Condition()
        self.pause_server = threading.Event()

    def _get_catalog(self) -> List[CatalogEntry]:
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

    def startup(self) -> Server:
        """
        Starts the Peer server component. Two paths could occur:
        - The peer becomes the leader, all traffic will flow through it
        - The peer becomes a follower, will contact the leader for all CRUD operations

        Returns:
            Server: An instance of the peers server
        """
        # TODO: Need to make more robust
        ckpt: str = f"calendar_{self.calendar_ident}_{self.peer_ident}.ckpt"
        txn: str = f"calendar_{self.calendar_ident}_{self.peer_ident}.txn"

        time.sleep(time.time() % random.randint(1, 5))

        server = Server(
            calendar_ident=self.calendar_ident,
            peer_ident=self.peer_ident,
            ckpt_path=ckpt,
            txn_path=txn,
        )

        self.own_host = server.host
        self.own_port = server.port

        peer_list = self.discovery_peers()
        self.log.info(f"{self.peer_ident} found {len(peer_list)} peers")
        # Staring off the network
        if not peer_list:
            server.mode = ServerMode.LEADER
            server.leaders_address = (self.own_host, self.own_port)
            self.log.info(f"{self.peer_ident} is the leader")
            return server

        # Query Peers for leaders address
        for host, port, name in peer_list:
            self.log.info(f"Attempting connection with {name}, {host}:{port}")
            try:
                with Client(
                    client_name=self.peer_ident,
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
                client_name=self.peer_ident,
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

    def create(self) -> Optional[int]:
        """If this peer is the leader, takes the lock on the calendar and updates it directly. Otherwise, uses the RPC stub on the client to update the calendar on the server, then syncs with the server."""

        pass

    # TODO: Write all the calendar stubs, make the mutating ones wait on the election condition variable.

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
                    calendar, clock = self.client.sync()
                except TimeoutError:
                    self.call_election()
                    continue
                self.server.update(calendar, clock)

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
            client = Client(entry["host"], entry["port"], self.own_host, self.own_port)
            if client.call_election():
                # a. If OK received, wait for COORDINATE message on server forever. (In the event all peers die at this point and never recover, the application will have to be restarted by the user.)
                self.server.await_coordinate()
                break
        else:
            # b. If no OK received from any peer with higher PID (or if there are no peers with a higher PID), become the leader and send COORDINATE. Check the logical clock on all COORDINATE ACKs, and SYNC with highest one.
            self.server.set_mode(ServerMode.LEADER)

            clients = []
            for entry in lower_pids:
                client = Client(
                    entry["host"], entry["port"], self.own_host, self.own_port
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

        # 3. Wake the UI thread if it is waiting for the election to finish to place write.
        with self.election_cv:
            self.election_happening = False
            self.election_cv.notify()

    def start_server():
        """Creates a background thread that serves incoming requests to the peer's server, and syncs the local calendar with the leader's calendar in the event of an update."""
        # TODO: Still need to add actual background thread at init
        pass

    def stop_server():
        """Stops a background thread that serves incoming requests to the peer's server, and syncs the local calendar with the leader's calendar in the event of an update."""
        pass


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage {sys.argv[0]} <calendar-project> <peer-name>", file=sys.stderr)
        sys.exit(1)

    peer = Peer(calendar_ident=sys.argv[1], peer_ident=sys.argv[2])
    peer.run()
