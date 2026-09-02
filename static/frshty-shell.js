/* frshty v2 shell — Linear-ish rail + main layout.
 *
 * Each page wraps its body content in <frshty-shell :features="...">…</frshty-shell>
 * which renders the dark sidebar nav on the left and slots the page content
 * into the main area. The shell also injects the .ln-root class on its root
 * div so the CSS variables in frshty-v2.css cascade correctly.
 *
 * Pages still own their own page-level chrome (topbar / breadcrumb / hero)
 * because each page's information density is different and the shell can't
 * generalise that.
 */
(function () {
	const SECTIONS = [
		{ title: 'Workspace', links: [
			{ href: '/', label: 'Inbox', icon: '◉' },
			{ href: '/global', label: 'Global', icon: '◎' },
		]},
		{ title: 'Work', links: [
			{ href: '/tasks', label: 'Tasks', icon: '◈', match: ['/tasks', '/threads'] },
			{ href: '/reviews', label: 'Reviews', icon: '◐' },
			{ href: '/today', label: 'Today', icon: '☉' },
			{ href: '/wizard', label: 'Wizard', icon: '✦' },
			{ href: '/tickets', label: 'Tickets', icon: '◑' },
			{ href: '/prd', label: 'PRD', icon: '▤' },
			{ href: '/scheduled', label: 'Scheduled', icon: '◔' },
			{ href: '/slack', label: 'Slack', icon: '◕' },
		]},
		{ title: 'Admin', links: [
			{ href: '/timesheet', label: 'Timesheet', icon: '◒', feature: 'timesheet' },
			{ href: '/billing', label: 'Billing', icon: '◓', feature: 'billing' },
			{ href: '/claude', label: 'Claude', icon: '◍' },
			{ href: '/config', label: 'Config', icon: '◌' },
		]},
	];

	function pathMatches(path, href) {
		if (href === '/') return path === '/';
		return path === href || path.indexOf(href + '/') === 0;
	}

	// A link can claim more than one path. Threads is a view of Tasks rather
	// than its own rail item, so /threads has to keep the Tasks item lit.
	function linkActive(path, link) {
		const hrefs = link.match || [link.href];
		return hrefs.some(h => pathMatches(path, h));
	}

	// Map URL paths back to the breadcrumb crumbs the topbar renders. Looking up
	// by current path keeps the topbar consistent across pages without each
	// page having to know its own breadcrumb. Each entry is [section, page] —
	// the rail's section header is reused as the breadcrumb root.
	const CRUMBS = {
		'/': ['Workspace', 'Inbox'],
		'/global': ['Workspace', 'Global'],
		'/tasks': ['Work', 'Tasks'],
		'/threads': ['Work', 'Tasks', 'Threads'],
		'/reviews': ['Work', 'Reviews'],
		'/today': ['Work', 'Today'],
		'/wizard': ['Work', 'Wizard'],
		'/tickets': ['Work', 'Tickets'],
		'/prd': ['Work', 'PRD'],
		'/scheduled': ['Work', 'Scheduled'],
		'/slack': ['Work', 'Slack'],
		'/timesheet': ['Admin', 'Timesheet'],
		'/billing': ['Admin', 'Billing'],
		'/claude': ['Admin', 'Claude'],
		'/config': ['Admin', 'Config'],
	};

	function crumbsForPath(path) {
		if (CRUMBS[path]) return CRUMBS[path];
		// Detail routes — /tickets/DEV-475 → ["Work", "Tickets", "DEV-475"]
		const m = path.match(/^(\/tickets|\/reviews|\/prd|\/tasks|\/threads)\/(.+?)\/?$/);
		if (m && CRUMBS[m[1]]) return [...CRUMBS[m[1]], m[2]];
		return ['Workspace', 'Home'];
	}

	const FrshtyShell = {
		props: {
			features: { type: Object, default: () => ({}) },
			instance: { type: String, default: '' },
			userInitials: { type: String, default: 'DJ' },
			userName: { type: String, default: 'danial jaffry' },
			counts: { type: Object, default: () => ({}) },
			// Pages can override the breadcrumb (e.g., ticket_detail wants
			// "Tickets / DEV-475" with a clickable Tickets link rather than
			// the auto-derived 3-segment crumb).
			crumbs: { type: Array, default: null },
			// "wide" → no max-width on .ln-page-wide, for table-dense pages.
			width: { type: String, default: 'normal' },
		},
		computed: {
			sections() {
				const f = this.features || {};
				const path = window.location.pathname;
				return SECTIONS.map(section => ({
					title: section.title,
					links: section.links
						.filter(l => !l.feature || f[l.feature] !== false)
						.map(l => Object.assign({}, l, {
							active: linkActive(path, l),
							count: this.counts[l.label.toLowerCase()] || 0,
						})),
				}));
			},
			breadcrumbs() {
				return this.crumbs || crumbsForPath(window.location.pathname);
			},
		},
		// The topbar is sticky. Every other sticky panel on the page has to
		// start below it, so publish the measured height as --ln-topbar-h and
		// keep it correct when the actions row wraps on a narrow window.
		mounted() {
			this.$nextTick(() => {
				const bar = this.$el.querySelector('.ln-topbar');
				if (!bar) return;
				const publish = () => {
					const h = Math.round(bar.getBoundingClientRect().height);
					if (h > 0) document.documentElement.style.setProperty('--ln-topbar-h', h + 'px');
				};
				publish();
				if (window.ResizeObserver) {
					this._topbarObserver = new ResizeObserver(publish);
					this._topbarObserver.observe(bar);
				}
			});
		},
		beforeUnmount() {
			if (this._topbarObserver) this._topbarObserver.disconnect();
		},
		template: `
			<div class="ln-root">
				<aside class="ln-rail">
					<div class="ln-brand">
						<div class="ln-logo">f</div>
						<div>
							<div class="ln-brand-title">frshty</div>
							<div class="ln-brand-sub">{{ instance || 'aimyable' }}</div>
						</div>
					</div>
					<a class="ln-cmdk" href="/">
						<kbd>⌘</kbd><kbd>K</kbd>
						<span>Jump to…</span>
					</a>
					<template v-for="section in sections" :key="section.title">
						<div class="ln-nav-section">
							<div class="ln-nav-title">{{ section.title }}</div>
							<a v-for="l in section.links" :key="l.href"
								 :href="l.href"
								 :class="['ln-navitem', { on: l.active }]">
								<span class="ln-nav-icon">{{ l.icon }}</span>
								<span>{{ l.label }}</span>
								<span v-if="l.count" class="ln-nav-count">{{ l.count }}</span>
							</a>
						</div>
					</template>
					<div class="ln-rail-foot">
						<div class="ln-avatar">{{ userInitials }}</div>
						<div>
							<div class="ln-foot-name">{{ userName }}</div>
							<div class="ln-foot-sub">● live</div>
						</div>
					</div>
				</aside>
				<main class="ln-main">
					<div :class="['ln-page', width === 'wide' ? 'ln-page-wide' : '']">
						<div class="ln-topbar">
							<div class="ln-breadcrumb">
								<template v-for="(crumb, i) in breadcrumbs" :key="i">
									<span class="ln-bc-sep" v-if="i > 0">/</span>
									<span :class="i === breadcrumbs.length - 1 ? 'ln-bc-active' : ''">{{ crumb }}</span>
								</template>
							</div>
							<div class="ln-topbar-actions">
								<slot name="actions">
									<button class="ln-btn ln-btn-ghost" title="Filter view (not wired)">Filter</button>
									<button class="ln-btn ln-btn-ghost" title="Display options (not wired)">Display</button>
								</slot>
							</div>
						</div>
						<slot></slot>
					</div>
				</main>
			</div>
		`,
	};

	// Time-relative helper kept on window so existing pages can use it without
	// re-importing — frshty-nav.js had the same helper at $relTime.
	function relTime(ts) {
		const d = window.frshtyTime.toDate(ts);
		if (!d) return '';
		const secs = Math.floor((Date.now() - d.getTime()) / 1000);
		if (secs < 60) return 'just now';
		const mins = Math.floor(secs / 60);
		if (mins < 60) return `${mins}m ago`;
		const hrs = Math.floor(mins / 60);
		if (hrs < 24) return `${hrs}h ago`;
		const days = Math.floor(hrs / 24);
		if (days < 7) return `${days}d ago`;
		return d.toLocaleDateString();
	}

	window.FrshtyShell = FrshtyShell;
	window.relTime = relTime;

	// Compatibility wrapper around the existing frshtyApp helper from
	// frshty-nav.js: register <frshty-shell> globally and forward the
	// rest of the user's app config.
	const _prior = window.frshtyApp;
	window.frshtyApp = function (config) {
		const app = (_prior || ((c) => Vue.createApp(c)))(config);
		try {
			if (app && app.component) {
				app.component('frshty-shell', FrshtyShell);
				// Also expose $relTime globally
				if (app.config && app.config.globalProperties) {
					app.config.globalProperties.$relTime = relTime;
				}
			}
		} catch (e) {}
		return app;
	};
})();
