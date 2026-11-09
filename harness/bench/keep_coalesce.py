# SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
#
# SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
import json
import re
import sys
from pathlib import Path

LISTENING = re.compile(rb"keep: listening on ")
COALESCED = re.compile(rb"keep: stream flushes coalesced for up to (\d+) us([^\n]*)")
FANOUT = re.compile(rb"from (\d+) subscriber connections")


def effective(data):
    if not LISTENING.search(data):
        return {"started": False, "coalesce_us": None, "coalesce_fanout": None}
    line = COALESCED.search(data)
    if line is None:
        return {"started": True, "coalesce_us": 0, "coalesce_fanout": None}
    fanout = FANOUT.search(line.group(2))
    return {"started": True, "coalesce_us": int(line.group(1)), "coalesce_fanout": int(fanout.group(1)) if fanout else 0}


def main():
    logs = {}
    for arg in sys.argv[1:]:
        path = Path(arg)
        logs[path.name] = effective(path.read_bytes() if path.is_file() else b"")
    print(json.dumps({"effective": logs}, sort_keys=True))


main()
