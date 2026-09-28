(function () {
    function mountInstancePicker() {
        fetch('/api/gateway/instances').then(r => r.ok ? r.json() : null).then(d => {
            if (!d || !Array.isArray(d.instances) || !d.instances.length) return;
            const box = document.createElement('div');
            box.id = 'frshty-instance-picker';
            box.title = 'Instance';
            box.style.cssText = 'position:fixed;top:12px;right:16px;z-index:50;display:flex;'
                + 'align-items:center;gap:6px;padding:3px 4px 3px 10px;border-radius:6px;'
                + 'font:12px/1.4 system-ui,sans-serif;background:#111827;color:#9ca3af;'
                + 'border:1px solid #374151;box-shadow:0 2px 8px rgba(0,0,0,.35)';
            const label = document.createElement('label');
            label.htmlFor = 'frshty-instance-select';
            label.textContent = 'Instance';
            const select = document.createElement('select');
            select.id = 'frshty-instance-select';
            select.style.cssText = 'background:#1f2937;color:#f3f4f6;border:1px solid #4b5563;'
                + 'border-radius:4px;padding:1px 6px;font:inherit;cursor:pointer';
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
            box.appendChild(label);
            box.appendChild(select);
            document.body.appendChild(box);
            const room = document.createElement('style');
            room.textContent = '.ln-topbar-actions,.ln-mobilebar{padding-right:'
                + (box.offsetWidth + 16) + 'px}';
            document.head.appendChild(room);
        }).catch(() => {});
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', mountInstancePicker);
    } else {
        mountInstancePicker();
    }
})();
