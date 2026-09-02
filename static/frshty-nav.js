(function () {
    const LINKS = [
        { href: '/', label: 'Feed' },
        { href: '/global', label: 'Global' },
        { href: '/reviews', label: 'Reviews' },
        { href: '/today', label: 'Today' },
        { href: '/wizard', label: 'Wizard' },
        { href: '/tickets', label: 'Tickets' },
        { href: '/prd', label: 'PRD' },
        { href: '/scheduled', label: 'Scheduled' },
        { href: '/slack', label: 'Slack' },
        { href: '/timesheet', label: 'Timesheet', feature: 'timesheet' },
        { href: '/billing', label: 'Billing', feature: 'billing' },
        { href: '/claude', label: 'Claude' },
        { href: '/config', label: 'Config' },
    ];

    function pathMatches(path, href) {
        if (href === '/') return path === '/';
        return path === href || path.indexOf(href + '/') === 0;
    }

    const FrshtyNav = {
        props: {
            features: { type: Object, default: () => ({}) },
        },
        computed: {
            visibleLinks() {
                const f = this.features || {};
                const path = window.location.pathname;
                return LINKS
                    .filter(l => !l.feature || f[l.feature] !== false)
                    .map(l => Object.assign({}, l, { active: pathMatches(path, l.href) }));
            },
        },
        template: `
            <nav class="flex gap-1 text-sm">
                <a v-for="l in visibleLinks" :key="l.href" :href="l.href" :class="{ active: l.active }">{{ l.label }}</a>
            </nav>
        `,
    };

    const relTime = ts => window.frshtyTime.relTime(ts);

    const PrSubmitModal = {
        props: {
            open: { type: Boolean, default: false },
            repos: { type: Array, default: () => [] },
            submitting: { type: Boolean, default: false },
            ticketKey: { type: String, default: '' },
        },
        emits: ['close', 'submit'],
        setup(props, ctx) {
            const { ref, watch, computed } = Vue;
            const localRepos = ref([]);
            const activeName = ref(null);
            const regenerating = ref(false);
            const regenError = ref('');
            watch(() => props.repos, (newRepos) => {
                localRepos.value = (newRepos || []).map(r => Object.assign({}, r));
                activeName.value = localRepos.value[0]?.name || null;
            }, { immediate: true });
            const active = computed(() => localRepos.value.find(r => r.name === activeName.value));
            const submittable = computed(() => localRepos.value.filter(r => r.has_changes !== false));
            function submit() {
                ctx.emit('submit', {
                    repos: localRepos.value
                        .filter(r => r.has_changes !== false)
                        .map(r => ({ name: r.name, title: r.title, description: r.description })),
                });
            }
            function close() { ctx.emit('close'); }
            async function regenerate() {
                if (!props.ticketKey || regenerating.value) return;
                regenerating.value = true;
                regenError.value = '';
                try {
                    const r = await fetch(`/api/tickets/${props.ticketKey}/pr-info/regenerate`, { method: 'POST' });
                    if (!r.ok) {
                        regenError.value = (await r.json().catch(() => ({}))).error || 'regenerate failed';
                        return;
                    }
                    const jobId = (await r.json()).job_id;
                    const deadline = Date.now() + 180000;
                    while (Date.now() < deadline) {
                        await new Promise(res => setTimeout(res, 2000));
                        const jr = await fetch(`/api/jobs/${jobId}/live`);
                        if (jr.ok && (await jr.json()).done) break;
                    }
                    const pr = await fetch(`/api/tickets/${props.ticketKey}/pr-info`);
                    if (pr.ok) {
                        const data = await pr.json();
                        localRepos.value = (data.repos || []).map(x => Object.assign({}, x));
                        if (!localRepos.value.find(x => x.name === activeName.value)) {
                            activeName.value = localRepos.value[0]?.name || null;
                        }
                    }
                } catch (e) {
                    regenError.value = String(e);
                } finally {
                    regenerating.value = false;
                }
            }
            return { localRepos, activeName, active, submittable, submit, close, regenerating, regenError, regenerate };
        },
        template: `
            <teleport to="body">
                <div v-if="open" class="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
                    <div class="card p-6 w-[70vw] h-[85vh] mx-4 flex flex-col">
                        <div class="flex items-center justify-between mb-4 pb-4 border-b border-gray-700">
                            <h2 class="text-lg font-bold">Submit PR</h2>
                            <div class="flex items-center gap-3">
                                <button v-if="ticketKey" @click="regenerate" :disabled="regenerating || submitting"
                                        class="text-xs px-3 py-1.5 rounded border border-gray-600 text-gray-300 hover:text-white hover:border-gray-400"
                                        :class="{ 'opacity-50 cursor-not-allowed': regenerating || submitting }"
                                        title="Regenerate the PR title & description from the change manifest, using the same flow that first wrote it">
                                    {{ regenerating ? 'Regenerating…' : '↻ Regenerate body' }}
                                </button>
                                <button @click="close" class="text-gray-600 hover:text-white text-xl">×</button>
                            </div>
                        </div>
                        <div v-if="regenError" class="text-xs text-red-400 mb-2">{{ regenError }}</div>
                        <div v-if="!localRepos.length" class="text-sm text-gray-400 flex-1 flex items-center justify-center">No repos with meaningful changes.</div>
                        <template v-else>
                            <div :class="{ 'opacity-40 pointer-events-none': regenerating }" class="flex-1 flex flex-col overflow-hidden">
                            <div class="flex gap-1 mb-3 border-b border-gray-700">
                                <button v-for="r in localRepos" :key="r.name" @click="activeName = r.name"
                                        class="px-3 py-2 text-sm border-b-2"
                                        :class="r.name === activeName ? 'border-blue-500 text-white' : 'border-transparent text-gray-400 hover:text-gray-200'">
                                    {{ r.name }}
                                    <span v-if="r.has_changes === false" class="text-xs text-yellow-400" title="Nothing to push from this branch">(nothing to push)</span>
                                    <span v-else class="text-xs text-gray-500">({{ r.files_changed }} files)</span>
                                </button>
                            </div>
                            <div v-if="active" class="flex-1 flex flex-col overflow-hidden">
                                <div class="mb-1 text-xs text-gray-500">{{ active.name }} → {{ active.branch }}</div>
                                <div v-if="active.has_changes === false" class="mb-3 px-3 py-2 rounded text-xs" style="background:#2a2410;border:1px solid #eab308;color:#f0e0b0">
                                    This repo will not be submitted: {{ active.stale_reason }}. The title and description below were written when the branch still had changes.
                                </div>
                                <div class="mb-3">
                                    <label class="text-xs font-bold text-gray-400 block mb-2">Title</label>
                                    <input type="text" v-model="active.title" v-autofocus
                                           class="w-full bg-gray-900 border border-gray-700 rounded px-3 py-2 text-sm text-gray-200 focus:border-blue-500 focus:outline-none" />
                                </div>
                                <div class="flex-1 flex flex-col overflow-hidden">
                                    <label class="text-xs font-bold text-gray-400 block mb-2">Description</label>
                                    <textarea v-model="active.description"
                                              class="flex-1 bg-gray-900 border border-gray-700 rounded px-3 py-2 text-sm text-gray-200 focus:border-blue-500 focus:outline-none font-mono resize-none overflow-y-auto"></textarea>
                                </div>
                            </div>
                            </div>
                        </template>
                        <div class="flex gap-2 mt-4 pt-4 border-t border-gray-700">
                            <button @click="submit" :disabled="submitting || !submittable.length"
                                    class="flex-1 bg-green-700 hover:bg-green-600 text-white text-sm px-4 py-2 rounded"
                                    :class="{ 'opacity-50 cursor-not-allowed': submitting || !submittable.length }">
                                {{ submitting ? 'Submitting…' : (submittable.length < localRepos.length ? 'Submit ' + submittable.length + ' of ' + localRepos.length : 'Submit') }}
                            </button>
                            <button @click="close" class="flex-1 bg-gray-700 hover:bg-gray-600 text-white text-sm px-4 py-2 rounded">Cancel</button>
                        </div>
                    </div>
                </div>
            </teleport>
        `,
    };

    const AutofocusDirective = {
        mounted(el) {
            if (el && typeof el.focus === 'function') el.focus();
        },
    };

    window.FrshtyNav = FrshtyNav;
    window.PrSubmitModal = PrSubmitModal;

    window.frshtyApp = function (options) {
        const app = Vue.createApp(options);
        app.component('frshty-nav', FrshtyNav);
        app.component('pr-submit-modal', PrSubmitModal);
        app.directive('autofocus', AutofocusDirective);
        app.config.globalProperties.$relTime = relTime;
        app.config.globalProperties.$time = window.frshtyTime;
        return app;
    };
})();
