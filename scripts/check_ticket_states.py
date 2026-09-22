"""Report whether named tickets are still in an instance's upstream query.

    python3 scripts/check_ticket_states.py myproject:PROJ-1,PROJ-2 other:OTH-9
"""

import sys

sys.path.insert(0, "/app")
import core.config as cfg
from features.ticket_systems import make_ticket_system

USAGE = "usage: check_ticket_states.py <instance>:<TICKET>[,<TICKET>...] ..."


def parse(args: list[str]) -> list[tuple[str, list[str]]]:
    targets = []
    for arg in args:
        inst, _, raw = arg.partition(":")
        keys = [k for k in raw.split(",") if k]
        if not inst or not keys:
            raise SystemExit(USAGE)
        targets.append((inst, keys))
    if not targets:
        raise SystemExit(USAGE)
    return targets


for inst, keys in parse(sys.argv[1:]):
    c = cfg.load_config(f"config/{inst}.toml")
    ts = make_ticket_system(c)
    if not ts:
        print(f"{inst}: no ticket system")
        continue
    all_tickets = ts.fetch_tickets()
    by_key = {t["key"]: t for t in all_tickets}
    for k in keys:
        t = by_key.get(k)
        if t:
            print(f"{inst} {k}: status={t.get('status')} summary={t.get('summary', '')[:60]}")
        else:
            print(f"{inst} {k}: NOT in active upstream query (closed/done upstream)")
