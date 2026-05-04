(function () {
    var LINKS = [
        { href: '/', label: 'Feed' },
        { href: '/global', label: 'Global' },
        { href: '/reviews', label: 'Reviews' },
        { href: '/today', label: 'Today' },
        { href: '/tickets', label: 'Tickets' },
        { href: '/prd', label: 'PRD' },
        { href: '/scheduled', label: 'Scheduled' },
        { href: '/slack', label: 'Slack' },
        { href: '/timesheet', label: 'Timesheet', feature: 'timesheet' },
        { href: '/billing', label: 'Billing', feature: 'billing' },
        { href: '/config', label: 'Config' },
    ];

    function pathMatches(path, href) {
        if (href === '/') return path === '/';
        return path === href || path.indexOf(href + '/') === 0;
    }

    var FrshtyNav = {
        props: {
            features: { type: Object, default: function () { return {}; } },
        },
        computed: {
            visibleLinks: function () {
                var f = this.features || {};
                var path = window.location.pathname;
                return LINKS.filter(function (l) {
                    return !l.feature || f[l.feature] !== false;
                }).map(function (l) {
                    return Object.assign({}, l, { active: pathMatches(path, l.href) });
                });
            },
        },
        template:
            '<nav class="flex gap-1 text-sm">' +
            '<a v-for="l in visibleLinks" :key="l.href" :href="l.href" :class="{ active: l.active }">{{ l.label }}</a>' +
            '</nav>',
    };

    window.FrshtyNav = FrshtyNav;

    window.frshtyApp = function (options) {
        var app = Vue.createApp(options);
        app.component('frshty-nav', FrshtyNav);
        return app;
    };
})();
