(function () {
  if (!document.getElementById("lk-style")) {
    const el = document.createElement("style");
    el.id = "lk-style";
    el.textContent = ".lk { color: #93c5fd; text-decoration: underline; }";
    document.head.appendChild(el);
  }

  const TICKET_BLOCKLIST = new Set(["UTF", "SHA", "ISO", "RFC", "CVE", "GPT", "AES", "RSA", "TLS", "MD"]);

  function esc(s) {
    return s.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }

  function anchor(href, label) {
    return `<a href="${href}" target="_blank" class="lk">${label}</a>`;
  }

  function outsideAnchors(html, transform) {
    return html.split(/(<a [^>]*>.*?<\/a>)/g)
      .map((seg, i) => (i % 2 === 1 ? seg : transform(seg)))
      .join("");
  }

  window.frshtyLinkify = function (text, linkmap) {
    let t = esc(text || "");
    t = t.replace(/https?:\/\/[^\s<>()"']+[^\s<>()"'.,;:]/g, m => anchor(m, m));
    const repos = (linkmap && linkmap.repos) || {};
    const names = Object.keys(repos).sort((a, b) => b.length - a.length);
    for (const name of names) {
      const quoted = name.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
      const re = new RegExp("(^|[^\\w/-])(" + quoted + ")\\s*#(\\d+)", "g");
      t = outsideAnchors(t, seg => seg.replace(re, (m, pre, nm, num) =>
        pre + anchor(repos[name].pr.replace("{n}", num), `${nm} #${num}`)));
    }
    const mentioned = names.filter(n => text && text.includes(n));
    if (mentioned.length === 1) {
      const hashRe = /\b(?=[0-9a-f]{7,40}\b)(?=[0-9a-f]*[a-f])(?=[0-9a-f]*\d)[0-9a-f]+\b/g;
      t = outsideAnchors(t, seg => seg.replace(hashRe, m =>
        anchor(repos[mentioned[0]].commit.replace("{sha}", m), m)));
    }
    t = outsideAnchors(t, seg => seg.replace(/\b([A-Z]{2,6})-(\d+)\b/g, (m, p, n) =>
      TICKET_BLOCKLIST.has(p) ? m : anchor("/tickets/" + p + "-" + n, m)));
    return t;
  };
})();
