(async function () {
    try {
        const resp = await fetch('/api/config');
        if (!resp.ok) return;
        const config = await resp.json();
        const features = config.features || {};
        const gate = (href, enabled) => {
            if (enabled) return;
            document.querySelectorAll(`nav a[href="${href}"]`).forEach(a => a.remove());
        };
        gate('/timesheet', features.timesheet);
        gate('/billing', features.billing);
    } catch (e) {}
})();
