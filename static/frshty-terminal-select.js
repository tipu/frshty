function frshtyTerminalSelect(term) {
  const isMac = /^Mac/.test(navigator.platform);
  term.options.macOptionClickForcesSelection = true;
  const forced = (ev) => new MouseEvent(ev.type, {
    bubbles: true, cancelable: true, composed: true, view: ev.view, detail: ev.detail,
    screenX: ev.screenX, screenY: ev.screenY, clientX: ev.clientX, clientY: ev.clientY,
    button: ev.button, buttons: ev.buttons, ctrlKey: ev.ctrlKey, metaKey: ev.metaKey,
    shiftKey: isMac ? ev.shiftKey : true, altKey: isMac ? true : ev.altKey,
  });
  term.element.addEventListener("mousedown", (ev) => {
    if (!ev.isTrusted || (ev.button !== 0 && ev.button !== 2)) return;
    ev.stopImmediatePropagation();
    ev.preventDefault();
    ev.target.dispatchEvent(forced(ev));
  }, true);
  term.element.addEventListener("mouseup", (ev) => {
    if (!ev.isTrusted || (ev.button !== 0 && ev.button !== 2)) return;
    ev.stopImmediatePropagation();
    document.dispatchEvent(forced(ev));
  }, true);
}
