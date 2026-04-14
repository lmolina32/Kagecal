import os
import struct
import pickle
import logging
from typing import NamedTuple, Optional, BinaryIO

from .Calendar import Calendar, Repeats, Event, Day


class Transaction(NamedTuple):
    method: str
    identifier: int
    event: Optional[Event]


class PersistantHashTable:

    # Global module configs
    CKPT_PATH = "calendar.ckpt"
    NEW_CKPT_PATH = "calendar.new.ckpt"
    TXN_LOG_PATH = "calendar.txns"
    CKPT_THRESHOLD = 100

    def __init__(self, log_level: int = logging.INFO) -> None:
        # Logging
        log_format = "[%(levelname)s %(asctime)s %(module)s:%(lineno)d] %(message)s"
        logging.basicConfig(
            format=log_format,
            datefmt="%Y-%m-%d %H:%M:%S",
            level=log_level,
        )

        self.logger = logging.getLogger()

        # Initialize actual hash table. It is important that these happen in this order.
        self.txns_logged = 0
        self.calendar: Calendar = self._restore()
        self.txn_log_file = open(self.TXN_LOG_PATH, "ab")

    def __del__(self) -> None:
        self.txn_log_file.close()

    def create(
        self,
        name: str,
        start: int,
        end: int,
        description: Optional[str] = None,
        location: Optional[str] = None,
        repeats: Optional[Repeats] = None,
    ) -> Optional[int]:
        # 1. Create new event
        event = Event(name, start, end, description, location, repeats)
        ident = self.calendar.create(name, start, end, description, location, repeats)

        if ident is None:
            return None

        # 2. Log the transaction
        txn = Transaction("create", ident, event)
        self._log(txn)

        return ident

    def delete(self, ident: int) -> None:
        # 1. Delete event
        self.calendar.delete(ident)

        # 2. Log the transaction
        txn = Transaction("delete", ident, None)
        self._log(txn)

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
        # 1. Modify event
        event = Event(name, start, end, description, location, repeats)
        new_ident = self.calendar.modify(
            ident, name, start, end, description, location, repeats
        )

        if new_ident is None:
            return None

        # 2. Log the transaction
        txn = Transaction("modify", ident, event)
        self._log(txn)
        return new_ident

    def _restore(self) -> Calendar:
        """Restore the in-memory calendar and recover from server failure by scanning the checkpoint and log, and retrying a checkpoint if necessary. Updates the instance transaction counter to reflect replayed transactions."""

        # Rebuild calendar from checkpoint, or create a new one. If a checkpoint exists, we are certain that it is complete.
        calendar = Calendar()
        if os.path.isfile(self.CKPT_PATH):
            with open(self.CKPT_PATH, "rb") as file:
                calendar.events = pickle.load(file)

        # Case: Server crashed while checkpointing, leaving a stale "new" checkpoint file. Remove it, and try to checkpoint again.
        if os.path.isfile(self.NEW_CKPT_PATH):
            os.remove(self.NEW_CKPT_PATH)
            self._checkpoint()

        # Replay transaction log, skipping the trailing entry if it is malformed.
        if os.path.isfile(self.TXN_LOG_PATH):
            with open(self.TXN_LOG_PATH, "rb") as txn_log:

                for txn in self._read_transactions(txn_log):
                    self.txns_logged += 1
                    if not txn:
                        break
                    self.logger.debug(f"[Restore]: Replaying transaction {txn}")

                    match txn.method:
                        case "create":
                            calendar.create(**txn.event.__dict__)
                        case "delete":
                            calendar.delete(txn.identifier)
                        case "modify":
                            calendar.modify(ident=txn.identifier, **txn.event.__dict__)

        return calendar

    def _read_transactions(self, f: BinaryIO) -> Transaction | None:
        """Read bytes from transaction file and generate Transactions"""
        while True:
            # read header
            header = f.read(4)
            if not header:
                return None
            if len(header) < 4:
                break
            # get payload
            (size,) = struct.unpack("!I", header)
            payload = f.read(size)
            if len(payload) < size:
                break
            yield pickle.loads(payload)

    def _log(self, txn: Transaction) -> None:
        """Append a transaction to the log. If the log length exceeds CKPT_THRESHOLD, commit a checkpoint."""
        self.logger.debug(f"[Log] Logging transaction {txn}")
        txn_pickle = pickle.dumps(txn, protocol=pickle.HIGHEST_PROTOCOL)

        header = struct.pack("!I", len(txn_pickle))

        # Force changes to disk
        self.txn_log_file.write(header + txn_pickle)
        self.txn_log_file.flush()
        os.fsync(self.txn_log_file.fileno())

        self.txns_logged += 1

        if self.txns_logged >= self.CKPT_THRESHOLD:
            self._checkpoint()

    def _checkpoint(self) -> None:
        """Create a new checkpoint, reset the transaction log, clean stale files."""
        self.logger.info("[Checkpoint] Checkpointing...")

        # 1. Write Table to new checkpoint file
        with open(self.NEW_CKPT_PATH, "wb") as new_ckpt_file:
            pickle.dump(
                self.calendar.events, new_ckpt_file, protocol=pickle.HIGHEST_PROTOCOL
            )

            # Force changes to disk
            new_ckpt_file.flush()
            os.fsync(new_ckpt_file.fileno())

        # 2. Atomically rename the new checkpoint to the main checkpoint
        os.rename(self.NEW_CKPT_PATH, self.CKPT_PATH)

        # 3. Truncate the transaction log, reset transaction counter.
        self.txn_log_file.truncate(0)
        self.txns_logged = 0
        self.logger.info("[Checkpoint] done.")
