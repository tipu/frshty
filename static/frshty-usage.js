(function () {
    var counts = {};
    var page = window.location.pathname;
    var CLICKABLE = 'a, button, summary, select, [role="button"], ' +
        'input[type="submit"], input[type="button"], input[type="checkbox"], ' +
        'input[type="radio"], [data-usage]';

    function descriptor(el) {
        var tag = el.tagName.toLowerCase();
        var label = el.getAttribute('data-usage') || el.id ||
            el.getAttribute('aria-label') || '';
        if (!label && (tag === 'a' || tag === 'button' || tag === 'summary')) {
            label = (el.textContent || '').replace(/\s+/g, ' ').trim();
        }
        if (!label && tag === 'a') label = el.getAttribute('href') || '';
        if (!label && tag === 'input') label = el.name || el.value || el.type;
        if (!label && tag === 'select') label = el.name || '';
        label = String(label).slice(0, 60);
        return label ? tag + ':' + label : tag;
    }

    function bump(name) {
        counts[name] = (counts[name] || 0) + 1;
    }

    document.addEventListener('click', function (e) {
        var target = e.target;
        if (!target || !target.closest) return;
        var el = target.closest(CLICKABLE);
        if (el) bump(descriptor(el));
    }, true);

    document.addEventListener('submit', function (e) {
        var form = e.target;
        if (!form || !form.getAttribute) return;
        var label = form.getAttribute('data-usage') || form.id ||
            form.getAttribute('action') || page;
        bump('form:' + String(label).slice(0, 60));
    }, true);

    function flush() {
        var names = Object.keys(counts);
        if (!names.length) return;
        var events = names.map(function (n) {
            return { element: n, count: counts[n] };
        });
        counts = {};
        var body = JSON.stringify({ page: page, events: events });
        if (navigator.sendBeacon) {
            navigator.sendBeacon('/api/usage/ui',
                new Blob([body], { type: 'application/json' }));
        } else {
            fetch('/api/usage/ui', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: body,
                keepalive: true
            }).catch(function () {});
        }
    }

    setInterval(flush, 15000);
    window.addEventListener('pagehide', flush);
    document.addEventListener('visibilitychange', function () {
        if (document.visibilityState === 'hidden') flush();
    });
})();
