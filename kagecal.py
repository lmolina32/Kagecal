#!/usr/bin/env

import sys

from DistributedCalendar.Peer import Peer

if len(sys.argv) != 3:
    print(f"Usage {sys.argv[0]} <calendar-project> <peer-name>", file=sys.stderr)
    sys.exit(1)

peer = Peer(calendar_name=sys.argv[1], peer_name=sys.argv[2])
peer.run()
