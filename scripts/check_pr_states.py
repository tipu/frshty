import sys
sys.path.insert(0, "/app")
import core.config as cfg
from features.platforms import make_platform

c = cfg.load_config("config/nimbus.toml")
p = make_platform(c)
for repo, pr_id in [("nimbus-app-backend", 669), ("nimbus-app-backend", 678)]:
    info = p.get_pr_info(repo, pr_id)
    print(f"{repo}#{pr_id}: state={info.get('state')}")
