(function () {
    const PARAM = 'frshty_instance';
    const key = new URLSearchParams(window.location.search).get(PARAM);
    if (!key) return;

    function pin(u) {
        let url;
        try { url = new URL(String(u), window.location.href); } catch (e) { return u; }
        if (url.host !== window.location.host || !/^(https?|wss?):$/.test(url.protocol)) return u;
        if (url.searchParams.has(PARAM)) return u;
        url.searchParams.set(PARAM, key);
        return url.href;
    }

    const fetch0 = window.fetch;
    window.fetch = function (input, init) {
        if (input instanceof Request) return fetch0.call(this, new Request(pin(input.url), input), init);
        return fetch0.call(this, pin(input), init);
    };

    const WebSocket0 = window.WebSocket;
    window.WebSocket = class extends WebSocket0 {
        constructor(url, protocols) { super(pin(url), protocols); }
    };

    if (window.EventSource) {
        const EventSource0 = window.EventSource;
        window.EventSource = class extends EventSource0 {
            constructor(url, init) { super(pin(url), init); }
        };
    }

    for (const name of ['pushState', 'replaceState']) {
        const orig = history[name];
        history[name] = function (state, title, url) {
            return url === undefined || url === null
                ? orig.call(this, state, title)
                : orig.call(this, state, title, pin(url));
        };
    }
})();
