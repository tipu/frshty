(function () {
  const css = `
.wq { margin-top: 10px; background: #101018; border: 1px solid #3b3b4d; border-radius: 8px; padding: 10px 14px; }
.wq-chip { display: inline-block; font-size: 11px; padding: 1px 8px; border-radius: 4px; background: #312e81; color: #c7d2fe; margin-bottom: 4px; }
.wq-text { font-size: 14px; color: #e5e5e5; white-space: pre-wrap; margin: 2px 0 8px; }
.wq-opt { border: 1px solid #2a2a35; border-radius: 6px; padding: 6px 10px; margin: 4px 0; cursor: pointer; }
.wq-opt:hover { border-color: #93c5fd; background: #16202e; }
.wq-opt-sel { border-color: #93c5fd; background: #1e3a5f; }
.wq-label { font-size: 13px; color: #93c5fd; }
.wq-desc { font-size: 11px; color: #8a8a97; margin-top: 2px; }
.wq-q + .wq-q { margin-top: 12px; border-top: 1px solid #2a2a35; padding-top: 10px; }
.wq-send { margin-top: 8px; }
.wq-btn { font-size: 12px; padding: 3px 12px; border-radius: 4px; background: #1e3a5f; color: #93c5fd; cursor: pointer; }
.wq-btn-off { opacity: 0.4; cursor: default; }
.wq-hint { font-size: 11px; color: #666; margin-left: 8px; }
`;
  if (!document.getElementById("wq-style")) {
    const el = document.createElement("style");
    el.id = "wq-style";
    el.textContent = css;
    document.head.appendChild(el);
  }

  window.WorkQuestion = {
    props: { item: { type: Object, required: true } },
    emits: ["answer"],
    data() { return { sel: {}, busy: false }; },
    watch: {
      "item.pending_question"() { this.sel = {}; this.busy = false; },
    },
    computed: {
      questions() {
        if (!this.item || !this.item.pending_question) return [];
        try {
          return JSON.parse(this.item.pending_question).questions || [];
        } catch (e) {
          return [];
        }
      },
      manual() {
        return this.questions.length > 1 || this.questions.some(q => q.multiSelect);
      },
      ready() {
        return this.questions.every((q, i) => (this.sel[i] || []).length > 0);
      },
      hint() {
        return this.questions.some(q => q.multiSelect)
          ? "select every option that applies, then send" : "pick one option per question, then send";
      },
    },
    methods: {
      selected(qi, label) {
        return (this.sel[qi] || []).includes(label);
      },
      pick(qi, label) {
        const q = this.questions[qi];
        if (this.busy) return;
        if (!this.manual) {
          this.busy = true;
          this.$emit("answer", this.compose({ 0: [label] }));
          return;
        }
        const sel = (this.sel[qi] || []).slice();
        const i = sel.indexOf(label);
        if (i >= 0) sel.splice(i, 1);
        else if (q.multiSelect) sel.push(label);
        else sel.splice(0, sel.length, label);
        this.sel[qi] = sel;
      },
      compose(picks) {
        return this.questions.map((q, i) => {
          const a = (picks[i] || []).join(", ");
          if (!a) return "";
          const tag = (q.header || q.question || "answer").trim().slice(0, 80);
          return `${tag}: ${a}`;
        }).filter(Boolean).join(" | ");
      },
      send() {
        if (!this.ready || this.busy) return;
        this.busy = true;
        this.$emit("answer", this.compose(this.sel));
        this.sel = {};
      },
    },
    template: `
<div class="wq" v-if="questions.length">
  <div class="wq-q" v-for="(q, qi) in questions" :key="qi">
    <span class="wq-chip" v-if="q.header">{{ q.header }}</span>
    <div class="wq-text">{{ q.question }}</div>
    <div :class="['wq-opt', selected(qi, o.label) ? 'wq-opt-sel' : '']"
         v-for="o in q.options || []" :key="o.label" @click="pick(qi, o.label)">
      <div class="wq-label">{{ o.label }}</div>
      <div class="wq-desc" v-if="o.description">{{ o.description }}</div>
    </div>
  </div>
  <div class="wq-send" v-if="manual">
    <span :class="['wq-btn', ready ? '' : 'wq-btn-off']" @click="send">answer &amp; resume</span>
    <span class="wq-hint">{{ hint }}</span>
  </div>
</div>`,
  };
})();
