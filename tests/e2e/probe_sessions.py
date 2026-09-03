#!/usr/bin/env python3
"""Named sessions and an explicit command.

A client holds one capability per destination per session, so a second
`brd host` neither overwrites the first's nor orphans it. Both halves are
checked against a real daemon: a session runs the argv it was given, and an
older one is still reachable by any unambiguous prefix of its id. `brd new`
is the form that must never resume, and the name `brd rename` gives a session
lives on the daemon, so the next `brd ls` prints it.
"""

import os
import re
import subprocess
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import drive  # noqa: E402
from drive import BRD, DEST, Session, check  # noqa: E402

drive.require_gate()

DETACH = b"\x1d" + b"d"
# `sh`, not the login shell, so the probe's own syntax is POSIX whatever the
# user's `$SHELL` is — and so `brd ls` has something to name that a login
# shell would not be called.
SH = [BRD, DEST, "--", "sh"]


def listing():
    return subprocess.run(
        [BRD, "ls", DEST], capture_output=True, text=True, timeout=30
    ).stdout

def ids(text):
    """The first column of every row the table names a session in.

    `brd ls` prints a short id, which is also what a user would type back.
    """
    return [
        row.split()[0]
        for row in text.splitlines()
        if re.match(r"^[0-9a-f]{8}\s", row)
    ]


def start(tag):
    """A session whose shell remembers which one it is."""
    session = Session(argv=SH)
    session.send(f"TAG={tag}; : > {drive.MARK_PREFIX}_{tag}\n".encode())
    started = drive.mark_appears(tag)
    session.send(DETACH)
    session.wait()
    return started


results = []
drive.cold_start()
drive.clear_marks()

results.append(check("an explicit command runs instead of the login shell", start("SESSA")))
results.append(check("a second session starts beside the first", start("SESSB")))

table = listing()
named = ids(table)
results.append(check(f"both sessions are listed ({len(named)})", len(named) == 2))
results.append(
    check(
        f"the listing names what actually runs ({table.splitlines()[-1].split()[-1]!r})",
        "sh" in table,
    )
)

# Oldest first is not promised, so find the one that is not the newest: `brd`
# with no id resumes the newest, and the whole point is reaching the other one.
#
# Nothing is waited for before typing: a resume paints no screen, so the only
# thing that proves which shell answered is the shell answering.
newest = Session()
newest.send(b"printf 'WHICH<%s>\\n' \"$TAG\"\n")
newest.read_until(b"WHICH<SESS", timeout=10.0)
recent = re.search(rb"WHICH<(SESS[AB])>", newest.buf)
newest.send(DETACH)
newest.wait()
results.append(check("a bare brd resumes the newest session", recent is not None))

if recent:
    older = "SESSA" if recent.group(1) == b"SESSB" else "SESSB"
    wanted = None
    for candidate in named:
        probe = Session(argv=[BRD, "attach", DEST, candidate])
        probe.send(b"printf 'WHICH<%s>\\n' \"$TAG\"\n")
        probe.read_until(b"WHICH<SESS", timeout=10.0)
        found = re.search(rb"WHICH<(SESS[AB])>", probe.buf)
        probe.send(DETACH)
        probe.wait()
        if found and found.group(1).decode() == older:
            wanted = candidate
            break
    results.append(
        check(f"an older session is reachable by id prefix ({older})", wanted is not None)
    )

results.append(check("an empty prefix is refused rather than guessed", subprocess.run(
    [BRD, "attach", DEST, ""], capture_output=True, text=True, timeout=30
).returncode != 0))


def settled(count, timeout=10.0):
    """The listing once it names `count` sessions, or once the wait is over."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        rows = ids(listing())
        if len(rows) == count:
            return rows
        time.sleep(0.3)
    return ids(listing())


# A bare `brd` just landed in the newest session; this asks for one beside it,
# with no command to force the point — `--` already refuses to resume.
made = Session(argv=[BRD, "new", DEST])
rows = settled(3)
results.append(check(f"brd new starts a session rather than resuming one ({len(rows)})", len(rows) == 3))
made.send(DETACH)
made.wait()


def rename(session_id, name):
    return subprocess.run(
        [BRD, "rename", DEST, session_id, name], capture_output=True, text=True, timeout=30
    )


if rows:
    target = rows[0]
    results.append(check("a session is renamed by id prefix", rename(target[:4], "deploy").returncode == 0))
    # A second `brd ls`, over a connection of its own: the name is the
    # daemon's, not something the renaming client remembered.
    table = listing()
    header, *body = table.splitlines()
    results.append(
        check(
            "the name is listed beside the id that carries it",
            "NAME" in header
            and any(row.startswith(target) and "deploy" in row for row in body),
        )
    )
    results.append(
        check(
            "an unnamed session keeps its row",
            sum(1 for row in body if "deploy" in row) == 1 and len(body) == 3,
        )
    )
    results.append(check("an empty name clears it", rename(target, "").returncode == 0
                         and "NAME" not in listing().splitlines()[0]))

results.append(check("a name for no session is refused", rename("ffffffff", "x").returncode != 0))

# The sessions are the probe's, not the user's.
for session_id in ids(listing()):
    subprocess.run([BRD, "kill", DEST, session_id], capture_output=True, timeout=30)
time.sleep(0.5)
results.append(check("every session the probe made is gone", not ids(listing())))

print(f"\n{sum(results)}/{len(results)} checks passed", flush=True)
sys.exit(0 if all(results) else 1)
