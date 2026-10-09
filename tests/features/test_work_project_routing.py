import json
import pathlib
import shutil
import subprocess

import pytest

BOARD = pathlib.Path("templates/work.html")

HARNESS = r"""
const src = require("fs").readFileSync(process.argv[1], "utf8");
const script = src.slice(src.lastIndexOf("<script>") + 8, src.lastIndexOf("</script>"));
let options = null;
global.frshtyApp = o => { options = o; return { component() { return this; }, mount() {} }; };
global.PasteImages = { tray: () => ({}), pending: () => false };
global.localStorage = { getItem: () => null, setItem() {} };
global.location = { search: "" };
global.window = {};
new Function(script)();
const vm = Object.assign({}, options.data(), JSON.parse(process.argv[2]));
for (const [k, f] of Object.entries(options.methods)) vm[k] = f.bind(vm);
for (const [k, f] of Object.entries(options.computed)) {
  Object.defineProperty(vm, k, { get: () => f.call(vm) });
}
const out = { catalog: vm.catalog.map(e => [e.key, vm.hostName(e.host), e.local]), launches: {} };
for (const e of vm.catalog) {
  vm.intakeContexts = [];
  vm.toggleContext(e.key);
  out.launches[e.key] = [vm.hostName(vm.target), vm.intakeProjects().map(p => p.key)];
}
vm.intakeContexts = [];
out.idle = vm.hostName(vm.target);
vm.toggleContext("personal");
vm.toggleContext("labsite");
out.personalThenLabsite = [vm.hostName(vm.target), vm.intakeContexts.slice()];
vm.toggleContext("cobalt");
out.thenCobalt = [vm.hostName(vm.target), vm.intakeContexts.slice()];
vm.projectFilter = ["astroco"];
out.filterAstroco = Object.fromEntries(vm.hosts().map(h => [vm.hostName(h), vm.hostProjects(h)]));
out.astrocoTags = vm.projectsOf({ peer: "astroco", contexts: "frshty,slack_int" });
delete vm.peerMeta.frshty;
out.frshtyDown = vm.catalog.map(e => e.key);
out.frshtyDownFilter = vm.filterProjects;
vm.peers = vm.peers.filter(p => p.key !== "personal");
vm.intakeContexts = [];
out.noPersonal = [vm.target, vm.targetMeta.personalLoaded];
vm.toggleContext("cobalt");
out.noPersonalCobalt = [vm.hostName(vm.target), vm.targetMeta.personalLoaded];
console.log(JSON.stringify(out));
"""


def _project(key, repos=()):
    return {"key": key, "root": f"/w/{key}", "repos": list(repos), "primary": True}


def _meta(*projects):
    return {"projects": list(projects), "agents": ["claude"],
            "slackAvailable": False, "personalLoaded": True}


STATE = {
    "instanceName": "quartz",
    "boardInstance": "personal",
    "localMeta": _meta(_project("frshty"), _project("quartz", ["quartz"])),
    "peers": [{"key": k, "base_url": "", "label": k}
              for k in ("frshty", "apexco", "cobalt", "personal", "astroco")],
    "peerMeta": {
        "frshty": _meta(_project("frshty", ["frshty"])),
        "apexco": _meta(_project("apexco", ["app"]), _project("frshty")),
        "cobalt": _meta(_project("cobalt", ["cobalt"]), _project("frshty")),
        "personal": _meta(_project("cobalt"), _project("frshty"), _project("labsite"),
                          _project("personal")),
        "astroco": _meta(_project("frshty", ["portal"])),
    },
}


@pytest.fixture(scope="module")
def routed():
    node = shutil.which("node")
    if not node:
        pytest.skip("node is not installed")
    out = subprocess.run([node, "-e", HARNESS, str(BOARD.resolve()), json.dumps(STATE)],
                         capture_output=True, text=True, check=True)
    return json.loads(out.stdout)


class TestProjectRouting:
    def test_every_project_routes_to_its_host(self, routed):
        assert routed["launches"] == {
            "apexco": ["apexco", ["apexco"]],
            "astroco": ["astroco", ["frshty"]],
            "cobalt": ["cobalt", ["cobalt"]],
            "frshty": ["frshty", ["frshty"]],
            "labsite": ["personal", ["labsite"]],
            "personal": ["personal", ["personal"]],
            "quartz": ["quartz", ["quartz"]],
        }

    def test_the_catalog_lists_each_project_once(self, routed):
        assert [row[0] for row in routed["catalog"]] == [
            "apexco", "astroco", "cobalt", "frshty", "labsite", "personal", "quartz"]

    def test_a_task_without_a_project_launches_on_personal(self, routed):
        assert routed["idle"] == "personal"

    def test_a_second_project_of_the_same_host_joins_the_selection(self, routed):
        assert routed["personalThenLabsite"] == ["personal", ["personal", "labsite"]]

    def test_a_project_of_another_host_replaces_the_selection(self, routed):
        assert routed["thenCobalt"] == ["cobalt", ["cobalt"]]

    def test_a_project_whose_host_is_down_does_not_fall_back_to_personal(self, routed):
        assert "frshty" not in routed["frshtyDown"]
        assert "labsite" in routed["frshtyDown"]

    def test_a_project_whose_host_is_down_stays_in_the_filter(self, routed):
        assert "frshty" in routed["frshtyDownFilter"]

    def test_a_task_without_a_project_does_not_launch_on_the_board_host_without_personal(self, routed):
        assert routed["noPersonal"] == [None, False]

    def test_a_project_still_routes_to_its_host_without_personal(self, routed):
        assert routed["noPersonalCobalt"] == ["cobalt", True]

    def test_the_filter_sends_each_host_its_own_project_key(self, routed):
        assert routed["filterAstroco"] == {
            "quartz": "", "frshty": "", "apexco": "", "cobalt": "", "personal": "",
            "astroco": "frshty"}

    def test_an_astroco_task_shows_the_astroco_tag(self, routed):
        assert routed["astrocoTags"] == ["astroco"]


class TestBoardHidesHosts:
    def test_the_board_has_no_source_filter_and_no_host_picker(self):
        board = BOARD.read_text()
        assert "sourceFilter" not in board
        assert "setSource(" not in board
        assert "host:" not in board
        assert "⇄" not in board
