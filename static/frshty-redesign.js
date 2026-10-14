(function () {
  const STATES = {
    needs_you: { label: "Needs you", dot: "amber" },
    needs_ack: { label: "To acknowledge", dot: "green" },
    proposed: { label: "Proposed", dot: "violet" },
    agent_working: { label: "Working", dot: "blue" },
    failed_stale: { label: "Failed or stale", dot: "red" },
    waiting_external: { label: "Waiting external", dot: "ghost" },
    done: { label: "Done", dot: "done" },
    canceled: { label: "Canceled", dot: "ghost" },
  };
  const ORDER = ["needs_you", "needs_ack", "proposed", "agent_working", "failed_stale",
                 "waiting_external", "done", "canceled"];
  const ATTENTION = ["needs_you", "needs_ack", "proposed"];
  const DECLINED_REASON = "Proposal declined";

  function emptyGroups() {
    const g = {};
    for (const s of ORDER) g[s] = [];
    return g;
  }

  const RedesignCore = {
    data() {
      return {
        features: {}, instanceName: "", boardInstance: "",
        view: "board", searchText: "",
        groups: emptyGroups(), counts: {}, donePage: 1, donePages: 1,
        localMeta: null, peers: [], peerMeta: {}, peerErrors: {}, linkmaps: {},
        threads: [], connError: "", actError: "", loaded: false,
        intakeText: "", intakeError: "", intakeBusy: false, intakeContexts: [],
        intakeSlack: false, intakeCritical: false, intakeRepo: "",
        intakeAgent: localStorage.getItem("workAgent") || "claude",
        intakeTray: PasteImages.tray(),
        selectedKey: "", selectedStub: null, detail: null, detailError: "", detailFor: "",
        propText: {}, busy: {}, replyText: "", fupText: "", fupLaunched: null,
        objectiveOpen: false, timelineOpen: false,
      };
    },
    computed: {
      railCounts() {
        return { tasks: this.count("proposed") + this.count("needs_ack") + this.count("needs_you") };
      },
      items() {
        const out = [];
        for (const g of ORDER) for (const it of this.groups[g] || []) out.push(it);
        return out;
      },
      attentionCount() {
        return ATTENTION.reduce((n, g) => n + this.count(g), 0);
      },
      threadIndex() {
        const idx = {};
        for (const t of this.threads) {
          for (const task of t.tasks || []) idx[(t.peer || "local") + "#" + task.id] = t;
        }
        return idx;
      },
      selected() {
        if (!this.selectedKey) return null;
        return this.items.find(it => this.rowKey(it) === this.selectedKey) || this.selectedStub;
      },
      cur() {
        const live = this.detail && this.detailFor === this.selectedKey ? this.detail.item : null;
        if (!live) return this.selected;
        const row = this.selected || {};
        return Object.assign({}, row, live, { peer: row.peer || "", tickets: this.detail.tickets || row.tickets || [] });
      },
      projectMaps() {
        const maps = {};
        for (const h of this.hosts()) {
          const meta = this.hostMeta(h);
          if (!meta) continue;
          const name = this.hostName(h);
          const projects = meta.projects || [];
          const alias = !!name && projects.length === 1 && projects[0].key !== name;
          const toDisplay = {}, toLocal = {};
          for (const p of projects) {
            const key = alias ? name : p.key;
            toDisplay[p.key] = key;
            toLocal[key] = p;
          }
          maps[h] = { toDisplay, toLocal };
        }
        return maps;
      },
      projectKeys() {
        const keys = new Set();
        for (const m of Object.values(this.projectMaps)) for (const k of Object.keys(m.toLocal)) keys.add(k);
        return [...keys].sort();
      },
      catalog() {
        const entries = [];
        for (const key of this.projectKeys) {
          const host = this.projectHost(key);
          if (host === null) continue;
          const p = this.projectMaps[host].toLocal[key];
          entries.push({ key, host, local: p.key, root: p.root, repos: p.repos || [], primary: p.primary });
        }
        return entries;
      },
      target() {
        const first = this.catalog.find(e => e.key === this.intakeContexts[0]);
        if (first) return first.host;
        const home = this.hosts().find(h => this.hostName(h) === this.boardInstance);
        return home === undefined ? null : home;
      },
      targetMeta() {
        return (this.target !== null && this.hostMeta(this.target)) ||
               { projects: [], agents: ["claude"], slackAvailable: false, personalLoaded: false };
      },
      intakeRepos() {
        const names = [];
        for (const p of this.intakeProjects()) for (const r of p.repos || []) if (!names.includes(r)) names.push(r);
        return names;
      },
      timelineView() {
        const tl = (this.detail && this.detailFor === this.selectedKey && this.detail.timeline) || [];
        const rev = tl.slice().reverse();
        const visible = this.timelineOpen ? rev : rev.slice(0, 12);
        const grouped = [];
        for (const e of visible) {
          if (e.kind !== "tool") { grouped.push(e); continue; }
          const prev = grouped[grouped.length - 1];
          if (prev && prev.kind === "tools") prev.entries.push(e);
          else grouped.push({ kind: "tools", entries: [e], at: e.at });
        }
        return grouped;
      },
      timelineHidden() {
        const tl = (this.detail && this.detailFor === this.selectedKey && this.detail.timeline) || [];
        return this.timelineOpen ? 0 : Math.max(0, tl.length - 12);
      },
    },
    methods: {
      stateLabel(it) {
        if (!it) return "";
        if (it.state === "canceled" && it.stop_reason === DECLINED_REASON) return "Declined";
        if (it.state === "done") return it.operator_confirmed ? "Done · you confirmed" : "Done · agent-reported";
        return (STATES[it.state] || { label: it.state }).label;
      },
      groupLabel(g) { return (STATES[g] || { label: g }).label; },
      dot(state) { return "rd-dot rd-dot-" + ((STATES[state] || {}).dot || "ghost"); },
      count(g) {
        if (this.counts && this.counts[g] != null) return this.counts[g];
        return (this.groups[g] || []).length;
      },
      rowKey(it) { return (it.peer || "local") + "#" + it.id; },
      hosts() { return ["", ...this.peers.map(p => p.key)]; },
      hostName(h) { return h || this.instanceName; },
      hostMeta(h) { return h ? this.peerMeta[h] : this.localMeta; },
      projectHost(key) {
        const knows = h => h !== undefined && !!(this.projectMaps[h] && this.projectMaps[h].toLocal[key]);
        const hosts = this.hosts();
        const named = hosts.find(h => this.hostName(h) === key);
        if (named !== undefined) return knows(named) ? named : null;
        const home = hosts.find(h => this.hostName(h) === this.boardInstance);
        for (const h of [home, ...hosts]) if (knows(h)) return h;
        return null;
      },
      intakeProjects() {
        const m = this.projectMaps[this.target];
        if (!m) return [];
        return this.intakeContexts.map(k => m.toLocal[k]).filter(Boolean);
      },
      toggleContext(key) {
        const i = this.intakeContexts.indexOf(key);
        if (i >= 0) this.intakeContexts.splice(i, 1);
        else if (this.intakeContexts.length && this.projectHost(key) !== this.target) this.intakeContexts = [key];
        else this.intakeContexts.push(key);
        this.intakeContexts = this.intakeContexts.filter(k => this.projectHost(k) === this.target);
        this.syncIntake();
      },
      syncIntake() {
        if (this.intakeRepo && this.intakeRepo !== "__none__" && !this.intakeRepos.includes(this.intakeRepo)) this.intakeRepo = "";
        const agents = this.targetMeta.agents || ["claude"];
        if (!agents.includes(this.intakeAgent)) this.intakeAgent = agents[0];
      },
      projectsOf(it) {
        const m = this.projectMaps[it.peer || ""];
        return (it.contexts || "").split(",").map(c => c.trim().toLowerCase())
          .filter(c => c && c !== "slack_int")
          .map(c => (m && m.toDisplay[c]) || c);
      },
      threadOf(it) { return it ? this.threadIndex[this.rowKey(it)] || null : null; },
      peerBase(key) {
        const peer = this.peers.find(p => p.key === key);
        return peer ? peer.base_url : "";
      },
      apiUrl(peer, path) {
        return peer ? "/api/work/peers/" + encodeURIComponent(peer) + path : path;
      },
      taskHref(it, suffix) {
        return "/tasks/" + it.id + (suffix || "") + (it.peer ? "?peer=" + encodeURIComponent(it.peer) : "");
      },
      threadHref(t) {
        return "/threads/" + t.root_id + (t.peer ? "?peer=" + encodeURIComponent(t.peer) : "");
      },
      terminalHref(it) { return (it.peer ? this.peerBase(it.peer) : "") + "/tasks/" + it.id + "/terminal"; },
      terminalWindow(it) { return window.frshtyTerminalWindow(it.id, it.peer); },
      artifactViewer(it, a) { return (it.peer ? this.peerBase(it.peer) : "") + "/artifacts/" + a.id; },
      lkFor(it, text) {
        const peer = (it && it.peer) || "";
        return window.frshtyLinkify(text || "", this.linkmaps[peer] || null, peer ? this.peerBase(peer) : "");
      },
      note(text, n) {
        const line = (text || "").trim();
        const cap = n || 200;
        return line.length > cap ? line.slice(0, cap).trimEnd() + "…" : line;
      },
      progressNote(it) { return this.note(it.latest_progress || it.current_checkpoint); },
      stopNote(it) {
        if (it.state === "done" || it.state === "agent_working") return "";
        return this.note((it.stop_reason || "").split("\n").filter(l => !l.includes("PROGRESS:")).join("\n"));
      },
      ago(iso) { return window.relTime(iso); },
      stamp(it) { return String(it.completed_at || it.updated_at || ""); },
      typing(e) {
        const t = e.target;
        return t && (t.tagName === "INPUT" || t.tagName === "TEXTAREA" || t.tagName === "SELECT" || t.isContentEditable);
      },
      async loadConfig() {
        try {
          const cfg = await (await fetch("/api/config")).json();
          this.features = cfg.features || {};
          this.instanceName = (cfg.job && cfg.job.key) || "";
        } catch (e) {
          this.connError = "could not read /api/config: " + e.message;
        }
      },
      async loadPeers() {
        try {
          const r = await fetch("/api/work/peers");
          if (!r.ok) throw new Error("HTTP " + r.status);
          this.peers = (await r.json()).peers || [];
        } catch (e) {
          this.peers = [];
          this.connError = "could not read the peer list: " + e.message;
        }
      },
      async loadLinkmaps() {
        for (const h of this.hosts()) {
          try { this.linkmaps[h] = await (await fetch(this.apiUrl(h, "/api/work/linkmap"))).json(); } catch (e) {}
        }
      },
      async loadThreads() {
        const per = await Promise.all(this.hosts().map(async key => {
          try {
            const r = await fetch(this.apiUrl(key, "/api/work/threads"));
            if (!r.ok) throw new Error("HTTP " + r.status);
            return ((await r.json()).threads || []).map(t => Object.assign({}, t, { peer: key }));
          } catch (e) {
            return [];
          }
        }));
        this.threads = per.flat().sort((a, b) => (a.updated_at < b.updated_at) - (a.updated_at > b.updated_at));
      },
      boardQuery() {
        const p = new URLSearchParams();
        const q = this.searchText.trim();
        if (q) p.set("q", q);
        if (this.donePage > 1) p.set("done_page", this.donePage);
        if (this.view === "archive") p.set("archive", "1");
        const qs = p.toString();
        return qs ? "?" + qs : "";
      },
      async refresh() {
        const seq = ++this._seq;
        const wantPage = Math.max(1, this.donePage);
        const hosts = this.hosts();
        const qs = this.boardQuery();
        const results = await Promise.all(hosts.map(async peer => {
          try {
            const r = await fetch(this.apiUrl(peer, "/api/work/items" + qs));
            const d = await r.json().catch(() => ({}));
            if (!r.ok) throw new Error(d.error || ("HTTP " + r.status));
            return { peer, data: d, error: "" };
          } catch (e) {
            return { peer, data: null, error: e.message || String(e) };
          }
        }));
        if (seq !== this._seq) return;
        const groups = emptyGroups(), counts = {}, peerErrors = {};
        let donePages = 1, ok = 0;
        for (const r of results) {
          if (r.error) {
            if (r.peer) peerErrors[r.peer] = r.error;
            else this.connError = "Disconnected from frshty (" + r.error + "). Retrying every 15s.";
            continue;
          }
          ok += 1;
          const d = r.data;
          const meta = { projects: d.projects || [], agents: (d.agents || []).length ? d.agents : ["claude"],
                         slackAvailable: !!d.slack_available, personalLoaded: !!d.personal_loaded };
          if (r.peer) this.peerMeta[r.peer] = meta;
          else { this.localMeta = meta; this.boardInstance = d.board_instance || ""; this.connError = ""; }
          for (const g of ORDER) {
            if (g === "done" && (d.done_page || 1) !== wantPage) continue;
            for (const it of (d.groups || {})[g] || []) { it.peer = r.peer; groups[g].push(it); }
          }
          for (const g of Object.keys(d.counts || {})) counts[g] = (counts[g] || 0) + d.counts[g];
          donePages = Math.max(donePages, d.done_pages || 1);
        }
        this.peerErrors = peerErrors;
        if (!ok) return;
        for (const g of ORDER) groups[g].sort((a, b) => this.stamp(b).localeCompare(this.stamp(a)));
        this.groups = groups;
        this.counts = counts;
        this.donePages = donePages;
        this.donePage = Math.min(wantPage, donePages);
        this.loaded = true;
        this.syncIntake();
      },
      setDonePage(p) { this.donePage = p; this.refresh(); },
      setView(v) { this.view = v; this.donePage = 1; this.refresh(); },
      onSearch() {
        clearTimeout(this._searchT);
        this._searchT = setTimeout(() => { this.donePage = 1; this.refresh(); }, 300);
      },
      select(it) {
        if (!it) { this.selectedKey = ""; this.selectedStub = null; this.detail = null; this.syncHash(); return; }
        const key = this.rowKey(it);
        if (key === this.selectedKey) return;
        this.selectedKey = key;
        this.selectedStub = Object.assign({}, it);
        this.detail = null; this.detailError = ""; this.detailFor = "";
        this.replyText = ""; this.fupText = ""; this.fupLaunched = null;
        this.objectiveOpen = false; this.timelineOpen = false;
        this.syncHash();
        this.loadDetail();
      },
      syncHash() {
        const want = this.selectedKey ? "#" + this.selectedKey : "";
        if (location.hash !== want) history.replaceState(null, "", location.pathname + location.search + want);
      },
      restoreHash() {
        const m = location.hash.match(/^#([^#]+)#(\d+)$/);
        if (!m) return;
        const peer = m[1] === "local" ? "" : decodeURIComponent(m[1]);
        this.select({ id: Number(m[2]), peer, objective: "", state: "" });
      },
      async loadDetail() {
        const it = this.selected;
        if (!it) return;
        const key = this.rowKey(it);
        try {
          const r = await fetch(this.apiUrl(it.peer, "/api/work/items/" + it.id + "/detail"));
          const d = await r.json().catch(() => ({}));
          if (!r.ok) throw new Error(d.error || ("HTTP " + r.status));
          if (this.selectedKey !== key) return;
          this.detail = d; this.detailFor = key; this.detailError = "";
        } catch (e) {
          if (this.selectedKey === key) this.detailError = "Unable to load task #" + it.id + ": " + (e.message || String(e));
        }
      },
      navNext(list, step) {
        if (!list.length) return;
        const i = list.findIndex(it => this.rowKey(it) === this.selectedKey);
        const j = i < 0 ? 0 : Math.min(list.length - 1, Math.max(0, i + step));
        this.select(list[j]);
        this.$nextTick(() => {
          const el = document.querySelector("[data-rowkey='" + this.selectedKey + "']");
          if (el) el.scrollIntoView({ block: "nearest" });
        });
      },
      async post(it, path, body) {
        this.actError = "";
        try {
          const r = await fetch(this.apiUrl(it.peer, "/api/work/items/" + it.id + path), {
            method: "POST", headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body || {}),
          });
          const d = await r.json().catch(() => ({}));
          if (!r.ok) { this.actError = "#" + it.id + ": " + (d.error || ("HTTP " + r.status)); return null; }
          return d;
        } catch (e) {
          this.actError = "#" + it.id + ": " + e.message;
          return null;
        }
      },
      async after(it, leaving) {
        const key = this.rowKey(it);
        const order = this.navList ? this.navList.map(x => this.rowKey(x)) : [];
        await this.refresh();
        this.loadThreads();
        if (key !== this.selectedKey) return;
        const still = this.navList ? this.navList.find(x => this.rowKey(x) === key) : null;
        if (leaving && !still) {
          if (!this.autoAdvance) { this.select(null); return; }
          const at = order.indexOf(key);
          const next = order.slice(at + 1).concat(order.slice(0, Math.max(0, at)).reverse())
            .map(k => this.navList.find(x => this.rowKey(x) === k)).find(Boolean);
          if (next) { this.select(next); return; }
        }
        this.loadDetail();
      },
      async act(it, action, extra) {
        const busyKey = this.rowKey(it) + ":" + action;
        if (this.busy[busyKey]) return;
        this.busy[busyKey] = true;
        try {
          const d = await this.post(it, "/action", Object.assign({ action }, extra || {}));
          if (d) await this.after(it, ["ack", "archive", "decline", "cancel", "done"].includes(action));
        } finally {
          delete this.busy[busyKey];
        }
      },
      async approve(it) {
        const key = this.rowKey(it);
        if (this.busy[key + ":approve"]) return;
        this.busy[key + ":approve"] = true;
        try {
          const d = await this.post(it, "/approve", { agent: this.intakeAgent, objective: this.propText[key] ?? it.objective });
          if (d) { delete this.propText[key]; await this.after(it, true); }
        } finally {
          delete this.busy[key + ":approve"];
        }
      },
      cancel(it) {
        if (!confirm("Cancel task #" + it.id + "? The agent's terminal is killed. The task is marked canceled and goes straight to the archive.")) return;
        this.act(it, "cancel");
      },
      snooze(it) {
        this.act(it, "snooze", { until: new Date(Date.now() + 4 * 3600 * 1000).toISOString() });
      },
      async answer(it, text) {
        text = (text || "").trim();
        if (!text) return false;
        const d = await this.post(it, "/reply", { text, question: it.pending_question || "" });
        if (!d) return false;
        this.replyText = "";
        await this.after(it, false);
        return true;
      },
      async followup(it) {
        const text = this.fupText.trim();
        const key = this.rowKey(it) + ":followup";
        if (!text || this.busy[key]) return;
        this.busy[key] = true;
        try {
          const d = await this.post(it, "/followup", { text, images: [] });
          if (d) { this.fupText = ""; this.fupLaunched = d.item_id || null; await this.after(it, false); }
        } finally {
          delete this.busy[key];
        }
      },
      async archiveDone() {
        const n = this.count("done");
        if (!n || !confirm("Archive " + n + " completed task" + (n === 1 ? "" : "s") + "?")) return;
        for (const peer of this.hosts()) {
          try {
            const r = await fetch(this.apiUrl(peer, "/api/work/items/archive-completed"), { method: "POST" });
            if (!r.ok) this.actError = "archive failed on " + (peer || "local") + ": HTTP " + r.status;
          } catch (e) {
            this.actError = "archive failed on " + (peer || "local") + ": " + e.message;
          }
        }
        await this.refresh();
      },
      async intake() {
        const block = PasteImages.submitBlock(this.intakeTray);
        if (block) { this.intakeError = block; return; }
        if (!this.intakeText.trim() || this.intakeBusy) return;
        this.intakeError = ""; this.intakeBusy = true;
        try {
          const r = await fetch(this.apiUrl(this.target, "/api/work/intake"), {
            method: "POST", headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ text: this.intakeText, agent: this.intakeAgent,
                                   contexts: this.intakeProjects().map(p => p.key),
                                   slack: this.intakeSlack && this.targetMeta.slackAvailable,
                                   critical: this.intakeCritical,
                                   repo: this.intakeRepo === "__none__" ? "" : this.intakeRepo,
                                   no_worktree: this.intakeRepo === "__none__",
                                   images: PasteImages.payload(this.intakeTray) }),
          });
          const d = await r.json().catch(() => ({}));
          if (!r.ok) { this.intakeError = d.error || ("intake failed (HTTP " + r.status + ")"); return; }
          localStorage.setItem("workAgent", this.intakeAgent);
          this.intakeText = ""; this.intakeContexts = []; this.intakeSlack = false;
          this.intakeCritical = false; this.intakeRepo = "";
          PasteImages.clear(this.intakeTray);
          await this.refresh();
        } catch (e) {
          this.intakeError = "request failed: " + e.message + " — is frshty up?";
        } finally {
          this.intakeBusy = false;
        }
      },
      onEnter(e, submit) {
        if (e.isComposing || e.shiftKey || e.ctrlKey || e.metaKey || e.altKey) return;
        e.preventDefault();
        submit();
      },
      grow(el) {
        if (!el) return;
        el.style.height = "auto";
        el.style.height = el.scrollHeight + "px";
      },
      onPaste(tray, e) { if (tray) PasteImages.onPaste(tray, e); },
      focusRef(name) {
        this.$nextTick(() => {
          const el = document.querySelector("[data-focus='" + name + "']");
          if (el) { el.scrollIntoView({ block: "center" }); el.focus(); }
        });
      },
      primary(it) {
        if (!it) return;
        if (it.state === "proposed") this.approve(it);
        else if (it.state === "needs_ack") this.act(it, "ack");
        else if (it.state === "done" && !it.archived_at) this.act(it, "archive");
        else if (it.state === "needs_you" || it.pending_question) this.focusRef("reply");
      },
      onKey(e) {
        if (e.defaultPrevented || e.ctrlKey || e.metaKey || e.altKey) return;
        if (e.key === "Escape") {
          if (this.typing(e)) { e.target.blur(); return; }
          if (this.closeSelection) { this.closeSelection(); return; }
        }
        if (this.typing(e)) return;
        const it = this.cur;
        const list = this.navList || [];
        const keys = {
          j: () => this.navNext(list, 1), ArrowDown: () => this.navNext(list, 1),
          k: () => this.navNext(list, -1), ArrowUp: () => this.navNext(list, -1),
          "/": () => this.focusRef("search"),
          c: () => this.focusRef("compose"),
          a: () => this.primary(it),
          e: () => it && (it.state === "done" || it.state === "needs_ack") && !it.archived_at && this.act(it, "archive"),
          d: () => it && it.state === "proposed" && this.act(it, "decline"),
          r: () => it && this.focusRef("reply"),
          f: () => it && (it.state === "done" || it.state === "needs_ack") && this.focusRef("followup"),
          o: () => it && window.open(this.taskHref(it), "_blank"),
          t: () => it && it.last_tmux_key && window.open(this.terminalHref(it), this.terminalWindow(it)),
        };
        const fn = keys[e.key];
        if (!fn) return;
        e.preventDefault();
        fn();
      },
      boot() {
        this._seq = 0;
        const b = document.getElementById("boot-error");
        if (b) b.remove();
        window.addEventListener("keydown", e => this.onKey(e));
        Promise.all([this.loadConfig(), this.loadPeers()]).then(async () => {
          this.loadLinkmaps();
          await Promise.all([this.refresh(), this.loadThreads()]);
          this.restoreHash();
          setInterval(() => {
            this.refresh();
            this.loadThreads();
            if (this.selectedKey) this.loadDetail();
          }, 15000);
        });
      },
    },
  };

  const TaskPane = {
    props: { variant: { type: String, default: "split" } },
    computed: {
      c() { return this.$root; },
      it() { return this.$root.cur; },
      d() { return this.$root.detail && this.$root.detailFor === this.$root.selectedKey ? this.$root.detail : null; },
      thread() {
        if (!this.it) return null;
        return this.c.threadOf(this.it);
      },
      finished() { return this.it && (this.it.state === "done" || this.it.state === "needs_ack"); },
      running() { return this.it && ["agent_working", "needs_you", "failed_stale", "waiting_external"].includes(this.it.state); },
      session() { return this.d && this.d.session; },
      draftFollowups() { return ((this.d && this.d.followups) || []).filter(f => f.status === "draft"); },
      longObjective() { return this.it && (this.it.objective || "").length > 280; },
    },
    template: `
<div class="rd-pane" :class="'rd-pane-' + variant" v-if="it">
  <div class="rd-pane-head">
    <div class="rd-pane-meta">
      <span :class="c.dot(it.state)"></span>
      <span class="rd-state">{{ c.stateLabel(it) }}</span>
      <span class="rd-id">{{ it.peer ? it.peer + ' ' : '' }}#{{ it.id }}</span>
      <span class="rd-crit" :class="{ on: it.critical }" :title="it.critical ? 'drop the critical mark' : 'mark this task critical'"
            @click="c.act(it, it.critical ? 'critical_off' : 'critical_on')">{{ it.critical ? '★' : '☆' }}</span>
      <span class="rd-grow"></span>
      <span class="rd-muted" v-if="it.updated_at">{{ c.ago(it.updated_at) }}</span>
      <a class="rd-link" :href="c.taskHref(it)" target="_blank" title="open the full task page (o)">full page ↗</a>
    </div>
    <h2 class="rd-objective" :class="{ clamp: longObjective && !c.objectiveOpen }">{{ it.objective }}</h2>
    <button class="rd-link" v-if="longObjective" @click="c.objectiveOpen = !c.objectiveOpen">{{ c.objectiveOpen ? 'show less' : 'show all' }}</button>
    <div class="rd-tags">
      <span class="ln-tag" v-for="p in c.projectsOf(it)" :key="p">{{ p }}</span>
      <a class="ln-pill ln-pill-ticket" v-for="t in (it.tickets || [])" :key="t.instance + '/' + t.key" :href="t.url" target="_blank">◫ {{ t.key }}</a>
      <a class="ln-pill ln-pill-violet" v-if="thread" :href="c.threadHref(thread)" target="_blank">⑂ {{ thread.title }}</a>
      <span class="rd-muted" v-if="session && it.state !== 'canceled'">{{ !session.alive ? 'no terminal session' : session.agent_running ? '● agent running' : 'agent stopped' }}</span>
    </div>
  </div>

  <div class="rd-actions">
    <template v-if="it.state === 'proposed'">
      <button class="ln-btn ln-btn-primary" :disabled="c.busy[c.rowKey(it) + ':approve']" @click="c.approve(it)">Approve &amp; launch <kbd>a</kbd></button>
      <button class="ln-btn" @click="c.act(it, 'decline')">Decline <kbd>d</kbd></button>
    </template>
    <template v-else-if="it.state === 'needs_ack'">
      <button class="ln-btn ln-btn-primary" @click="c.act(it, 'ack')">Acknowledge <kbd>a</kbd></button>
      <button class="ln-btn" @click="c.act(it, 'archive')">Archive <kbd>e</kbd></button>
      <button class="ln-btn" @click="c.focusRef('followup')">Follow up <kbd>f</kbd></button>
      <button class="ln-btn ln-btn-ghost" @click="c.act(it, 'reopen')">Reopen</button>
    </template>
    <template v-else-if="it.state === 'done'">
      <button class="ln-btn ln-btn-primary" v-if="!it.archived_at" @click="c.act(it, 'archive')">Archive <kbd>e</kbd></button>
      <button class="ln-btn" v-else @click="c.act(it, 'unarchive')">Unarchive</button>
      <button class="ln-btn" @click="c.focusRef('followup')">Follow up <kbd>f</kbd></button>
      <button class="ln-btn ln-btn-ghost" @click="c.act(it, 'reopen')">Reopen</button>
    </template>
    <template v-else-if="it.state === 'canceled'">
      <button class="ln-btn" @click="c.act(it, 'reopen')">Reopen</button>
    </template>
    <template v-else>
      <a class="ln-btn" v-if="it.last_tmux_key" :href="c.terminalHref(it)" :target="c.terminalWindow(it)">Terminal <kbd>t</kbd></a>
      <button class="ln-btn ln-btn-ghost" @click="c.act(it, 'done')">Done</button>
      <button class="ln-btn ln-btn-ghost" @click="c.snooze(it)">Snooze 4h</button>
      <button class="ln-btn ln-btn-ghost" v-if="it.state === 'failed_stale' || it.state === 'waiting_external'" @click="c.act(it, 'reopen')">Reopen</button>
      <button class="ln-btn ln-btn-ghost" @click="c.cancel(it)">Cancel</button>
    </template>
    <span class="rd-err" v-if="c.actError">{{ c.actError }}</span>
  </div>

  <div class="rd-sec rd-sec-warn" v-if="c.stopNote(it) && !it.pending_question" v-html="c.lkFor(it, c.stopNote(it))"></div>

  <div class="rd-sec" v-if="it.state === 'needs_you' || it.pending_question">
    <work-question v-if="it.pending_question" :item="it" @answer="(text, done) => c.answer(it, text).then(done)"></work-question>
    <form @submit.prevent="c.answer(it, c.replyText)">
      <input class="rd-input" data-focus="reply" v-model="c.replyText"
             :placeholder="it.pending_question ? 'Other — type your own answer… (r)' : 'Answer and resume… (r)'" />
    </form>
  </div>

  <div class="rd-sec" v-if="it.state === 'proposed'">
    <div class="rd-h">Objective the agent starts on</div>
    <textarea class="rd-input" rows="4" :value="c.propText[c.rowKey(it)] ?? it.objective"
              @input="c.propText[c.rowKey(it)] = $event.target.value"></textarea>
    <details v-if="it.launch_brief"><summary class="rd-muted">evidence frshty acted on</summary><pre class="rd-pre">{{ it.launch_brief }}</pre></details>
  </div>

  <div class="rd-sec rd-progress" v-if="c.progressNote(it) && it.state !== 'done'">
    <div class="rd-h">Latest progress</div>{{ c.progressNote(it) }}
  </div>

  <div class="rd-sec" v-if="it.summary">
    <div class="rd-h">Summary <a class="rd-link" :href="c.taskHref(it, '/summary')" target="_blank">↗</a></div>
    <div class="rd-summary" v-html="c.lkFor(it, it.summary)"></div>
  </div>

  <div class="rd-sec" v-if="finished">
    <div class="rd-h">Follow up</div>
    <textarea class="rd-input" rows="2" data-focus="followup" v-model="c.fupText"
              @keydown.enter="c.onEnter($event, () => c.followup(it))"
              placeholder="Next objective. It keeps this task's projects and agent. Enter launches. (f)"></textarea>
    <div class="rd-warn" v-if="$questionWarning(c.fupText)">{{ $questionWarning(c.fupText) }}</div>
    <div class="rd-muted" v-if="c.fupLaunched">Follow-up launched as <a class="rd-link" :href="c.taskHref({ id: c.fupLaunched, peer: it.peer })">#{{ c.fupLaunched }}</a>.</div>
  </div>

  <div class="rd-sec" v-if="running && it.state !== 'needs_you' && !it.pending_question">
    <div class="rd-h">Correct the agent</div>
    <form @submit.prevent="c.answer(it, c.replyText)">
      <input class="rd-input" data-focus="reply" v-model="c.replyText" placeholder="Send text into the conversation… (r)" />
    </form>
  </div>

  <div class="rd-sec" v-if="draftFollowups.length">
    <a class="rd-link" :href="c.taskHref(it)" target="_blank">{{ draftFollowups.length }} drafted follow-up{{ draftFollowups.length === 1 ? '' : 's' }} wait on the full page ↗</a>
  </div>

  <div class="rd-sec" v-if="thread">
    <div class="rd-h">Thread · {{ thread.task_count }} tasks</div>
    <div class="rd-chain">
      <div class="rd-chain-row" v-for="t in thread.tasks" :key="t.id" :class="{ cur: t.id === it.id }"
           @click="c.select({ id: t.id, peer: thread.peer, objective: t.objective, state: t.state })">
        <span :class="c.dot(t.state)"></span><span class="rd-id">#{{ t.id }}</span><span class="rd-chain-obj">{{ t.objective }}</span>
      </div>
    </div>
  </div>

  <div class="rd-sec" v-if="d && d.artifacts && d.artifacts.length">
    <div class="rd-h">Artifacts</div>
    <div class="rd-art" v-for="a in d.artifacts" :key="a.id"><a class="rd-link" :href="c.artifactViewer(it, a)" target="_blank">{{ a.path.split('/').pop() }}</a> <span class="rd-muted">{{ a.note }}</span></div>
  </div>

  <div class="rd-sec" v-if="d && d.followup_children && d.followup_children.length">
    <div class="rd-h">Followed up by</div>
    <div v-for="ch in d.followup_children" :key="ch.id" class="rd-chain-row" @click="c.select({ id: ch.id, peer: it.peer, objective: ch.objective, state: ch.state })">
      <span :class="c.dot(ch.state)"></span><span class="rd-id">#{{ ch.id }}</span><span class="rd-chain-obj">{{ ch.objective }}</span>
    </div>
  </div>

  <div class="rd-sec" v-if="d && d.timeline && d.timeline.length">
    <div class="rd-h">What happened · newest first</div>
    <div class="rd-tl">
      <template v-for="(e, i) in c.timelineView" :key="i">
        <div v-if="e.kind === 'prompt'" class="rd-tl-prompt"><span class="rd-muted">{{ c.ago(e.at) }}</span> ❯ {{ c.note(e.text, 600) }}</div>
        <div v-else-if="e.kind === 'text'" class="rd-tl-text"><span class="rd-muted">{{ c.ago(e.at) }}</span> <span v-html="c.lkFor(it, e.text)"></span></div>
        <details v-else class="rd-tl-tools"><summary>{{ e.entries.length }} tool call{{ e.entries.length === 1 ? '' : 's' }}</summary>
          <div v-for="(t, k) in e.entries" :key="k">{{ t.name }} {{ t.arg }}</div></details>
      </template>
    </div>
    <button class="rd-link" v-if="c.timelineHidden" @click="c.timelineOpen = true">show {{ c.timelineHidden }} older entries</button>
  </div>
  <div class="rd-muted rd-sec" v-else-if="!d && !c.detailError">Loading the transcript…</div>
  <div class="rd-err rd-sec" v-if="c.detailError">{{ c.detailError }} <button class="rd-link" @click="c.loadDetail()">retry</button></div>
</div>`,
  };

  const Compose = {
    props: { compact: { type: Boolean, default: false } },
    computed: {
      c() { return this.$root; },
      projects() {
        const c = this.$root;
        const primary = c.catalog.filter(p => p.primary !== false);
        const extra = c.catalog.filter(p => p.primary === false && (this.more || c.intakeContexts.includes(p.key)));
        return [...primary, ...extra];
      },
      hasExtra() { return this.$root.catalog.some(p => p.primary === false); },
      open() { return !this.compact || this.focused || !!this.$root.intakeText; },
    },
    data() { return { more: false, focused: false }; },
    template: `
<form class="rd-compose" @submit.prevent="c.intake()">
  <textarea class="rd-input" rows="1" data-focus="compose" v-model="c.intakeText"
            :disabled="!c.targetMeta.personalLoaded || c.intakeBusy"
            @focus="focused = true" @keydown.enter="c.onEnter($event, c.intake)"
            @input="c.grow($event.target)" @paste="c.onPaste(c.intakeTray, $event)"
            placeholder="New task: describe an outcome (c)"></textarea>
  <paste-tray :tray="c.intakeTray"></paste-tray>
  <div class="rd-compose-opts" v-if="open">
    <span class="ln-chipsel">agent:
      <select v-model="c.intakeAgent"><option v-for="a in c.targetMeta.agents" :key="a" :value="a">{{ a }}</option></select>
    </span>
    <span class="ln-chipsel" v-for="p in projects" :key="p.key" :title="p.root"
          :class="{ on: c.intakeContexts.includes(p.key) }" @click="c.toggleContext(p.key)">{{ p.key }}</span>
    <span class="ln-chipsel" v-if="hasExtra" @click="more = !more">{{ more ? '− less' : '+ context' }}</span>
    <span class="ln-chipsel" v-if="c.targetMeta.slackAvailable" :class="{ on: c.intakeSlack }" @click="c.intakeSlack = !c.intakeSlack">slack_int</span>
    <span class="ln-chipsel crit" :class="{ on: c.intakeCritical }" @click="c.intakeCritical = !c.intakeCritical">★ critical</span>
    <span class="ln-chipsel">worktree:
      <select v-model="c.intakeRepo">
        <option value="">auto</option>
        <option v-for="r in c.intakeRepos" :key="r" :value="r">{{ r }}</option>
        <option value="__none__">no worktree</option>
      </select>
    </span>
    <span class="rd-grow"></span>
    <button type="submit" class="ln-btn ln-btn-primary ln-btn-sm"
            :disabled="!c.targetMeta.personalLoaded || c.intakeBusy || !c.intakeText.trim()">{{ c.intakeBusy ? 'launching…' : '↗ Launch task' }}</button>
  </div>
  <div class="rd-warn" v-if="$questionWarning(c.intakeText)">{{ $questionWarning(c.intakeText) }}</div>
  <div class="rd-err" v-if="c.intakeError">{{ c.intakeError }}</div>
  <div class="rd-muted" v-if="c.loaded && !c.targetMeta.personalLoaded">the selected project cannot launch a task right now</div>
</form>`,
  };

  const DesignSwitch = {
    props: { current: { type: String, required: true } },
    template: `
<div class="ln-segment" title="three redesigns of the tasks, task and thread pages">
  <a class="ln-seg-btn" href="/tasks">Classic</a>
  <a class="ln-seg-btn" :class="{ on: current === '1' }" href="/tasks/design/1">1 · Inbox</a>
  <a class="ln-seg-btn" :class="{ on: current === '2' }" href="/tasks/design/2">2 · Table</a>
  <a class="ln-seg-btn" :class="{ on: current === '3' }" href="/tasks/design/3">3 · Lanes</a>
</div>`,
  };

  window.RedesignCore = RedesignCore;
  window.RedesignStates = { STATES, ORDER, ATTENTION };
  window.redesignApp = function (config) {
    return frshtyApp(Object.assign({ mixins: [RedesignCore] }, config))
      .component("rd-task", TaskPane)
      .component("design-switch", DesignSwitch)
      .component("rd-compose", Compose)
      .component("work-question", window.WorkQuestion)
      .component("paste-tray", window.PasteTray);
  };
})();
