(function () {
    function mountInstancePicker() {
        fetch('/api/gateway/instances').then(r => r.ok ? r.json() : null).then(d => {
            if (!d || !Array.isArray(d.instances) || !d.instances.length) return;
            const bar = document.createElement('div');
            bar.id = 'frshty-instance-bar';
            bar.style.cssText = 'display:flex;align-items:center;gap:8px;padding:4px 16px;'
                + 'font:12px/1.4 system-ui,sans-serif;background:#111827;color:#9ca3af;'
                + 'border-bottom:1px solid #374151';
            const label = document.createElement('label');
            label.htmlFor = 'frshty-instance-select';
            label.textContent = 'Instance';
            const select = document.createElement('select');
            select.id = 'frshty-instance-select';
            select.style.cssText = 'background:#1f2937;color:#f3f4f6;border:1px solid #4b5563;'
                + 'border-radius:4px;padding:1px 6px;font:inherit';
            d.instances.forEach(i => {
                const opt = document.createElement('option');
                opt.value = i.key;
                opt.textContent = i.label;
                opt.selected = i.key === d.current;
                select.appendChild(opt);
            });
            select.addEventListener('change', () => {
                const next = window.location.pathname + window.location.search;
                window.location.href = '/api/gateway/select?key=' + encodeURIComponent(select.value)
                    + '&next=' + encodeURIComponent(next);
            });
            bar.appendChild(label);
            bar.appendChild(select);
            document.body.insertBefore(bar, document.body.firstChild);
        }).catch(() => {});
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', mountInstancePicker);
    } else {
        mountInstancePicker();
    }
})();
