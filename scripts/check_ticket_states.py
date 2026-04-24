import sys
sys.path.insert(0, "/app")
import core.config as cfg
from features.ticket_systems import make_ticket_system

for inst, keys in [("aimyable", ["DEV-437", "DEV-450"]), ("nectar", ["NEC-3039"])]:
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
