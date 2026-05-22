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
			{ href: '/reviews', label: 'Reviews', icon: '◐' },
			{ href: '/today', label: 'Today', icon: '☉' },
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

	const FrshtyShell = {
		props: {
			features: { type: Object, default: () => ({}) },
			instance: { type: String, default: '' },
			userInitials: { type: String, default: 'DJ' },
			userName: { type: String, default: 'danial jaffry' },
			counts: { type: Object, default: () => ({}) },
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
							active: pathMatches(path, l.href),
							count: this.counts[l.label.toLowerCase()] || 0,
						})),
				}));
			},
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
					<slot></slot>
				</main>
			</div>
		`,
	};

	// Time-relative helper kept on window so existing pages can use it without
	// re-importing — frshty-nav.js had the same helper at $relTime.
	function relTime(ts) {
		if (!ts) return '';
		const d = new Date(ts);
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
