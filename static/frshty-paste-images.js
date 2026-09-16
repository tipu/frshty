(function () {
  const css = `
.ln-attach { display: flex; flex-wrap: wrap; gap: 8px; margin-top: 8px; }
.ln-attach-item { position: relative; display: inline-block; }
.ln-attach-item img { display: block; width: 56px; height: 56px; object-fit: cover; border: 1px solid var(--ln-border-hi, #2a2a35); border-radius: 6px; background: var(--ln-bg, #0b0b10); }
.ln-attach-wait { display: flex; align-items: center; justify-content: center; width: 56px; height: 56px; border: 1px dashed var(--ln-border-hi, #2a2a35); border-radius: 6px; background: var(--ln-bg, #0b0b10); color: var(--ln-text-2, #8a8a97); font-size: 10px; }
.ln-attach-x { position: absolute; top: -6px; right: -6px; width: 17px; height: 17px; padding: 0; line-height: 15px; text-align: center; border-radius: 50%; border: 1px solid var(--ln-border-hi, #2a2a35); background: var(--ln-bg, #0b0b10); color: var(--ln-text, #e0e0e0); font-size: 11px; cursor: pointer; }
.ln-attach-error { font-size: 11px; color: var(--ln-red, #fca5a5); margin-top: 4px; }
`;
  if (!document.getElementById("paste-images-style")) {
    const el = document.createElement("style");
    el.id = "paste-images-style";
    el.textContent = css;
    document.head.appendChild(el);
  }

  const TYPES = ["image/png", "image/jpeg", "image/gif", "image/webp"];
  const MAX_IMAGES = 8;
  const MAX_BYTES = 10 * 1024 * 1024;
  const PENDING_MESSAGE = "a pasted image is still being read; launch again in a moment";
  let seq = 0;

  function tray() {
    return { images: [], error: "" };
  }

  function add(tray, file) {
    const name = file.name || "pasted image";
    if (!TYPES.includes(file.type)) {
      tray.error = "cannot attach " + (file.type || "that file") +
                   " — paste a png, jpeg, gif or webp image";
      return;
    }
    if (tray.images.length >= MAX_IMAGES) {
      tray.error = "at most " + MAX_IMAGES + " images per task";
      return;
    }
    if (file.size > MAX_BYTES) {
      tray.error = name + " is over the " + Math.round(MAX_BYTES / 1048576) + " MB limit";
      return;
    }
    const id = ++seq;
    tray.images.push({ id, name, type: file.type, url: "", data: "" });
    const drop = () => {
      const at = tray.images.findIndex(im => im.id === id);
      if (at >= 0) tray.images.splice(at, 1);
      tray.error = name + " could not be read";
    };
    const reader = new FileReader();
    reader.onload = () => {
      const entry = tray.images.find(im => im.id === id);
      if (!entry) return;
      const url = String(reader.result || "");
      const data = url.slice(url.indexOf(",") + 1);
      if (!data) { drop(); return; }
      entry.url = url;
      entry.data = data;
    };
    reader.onerror = drop;
    reader.readAsDataURL(file);
  }

  function imageFiles(e) {
    const items = (e.clipboardData && e.clipboardData.items) || [];
    return Array.from(items)
      .filter(it => it.kind === "file" && (it.type || "").startsWith("image/"))
      .map(it => it.getAsFile())
      .filter(Boolean);
  }

  function onPaste(tray, e) {
    const files = imageFiles(e);
    if (!files.length) return;
    e.preventDefault();
    files.forEach(f => add(tray, f));
  }

  function remove(tray, i) {
    tray.images.splice(i, 1);
  }

  function clear(tray) {
    tray.images.splice(0, tray.images.length);
    tray.error = "";
  }

  function pending(tray) {
    return tray.images.some(im => !im.data);
  }

  function submitBlock(tray) {
    if (pending(tray)) return PENDING_MESSAGE;
    if (!tray.error) return "";
    const message = tray.error + " — press again to launch without it";
    tray.error = "";
    return message;
  }

  function payload(tray) {
    return tray.images.map(im => ({ name: im.name, type: im.type, data: im.data }));
  }

  window.PasteImages = {
    TYPES, MAX_IMAGES, MAX_BYTES, PENDING_MESSAGE,
    tray, add, imageFiles, onPaste, remove, clear, pending, submitBlock, payload,
  };

  window.PasteTray = {
    props: { tray: { type: Object, required: true } },
    methods: {
      drop(i) { remove(this.tray, i); },
    },
    template: `
<div v-if="tray.images.length || tray.error">
  <div class="ln-attach" v-if="tray.images.length">
    <span class="ln-attach-item" v-for="(im, i) in tray.images" :key="im.id">
      <img v-if="im.url" :src="im.url" :alt="im.name"
           :title="im.name + ' — this image goes up with the task'">
      <span v-else class="ln-attach-wait" :title="'reading ' + im.name">reading…</span>
      <button type="button" class="ln-attach-x" :title="'remove ' + im.name"
              @click="drop(i)">×</button>
    </span>
  </div>
  <div class="ln-attach-error" v-if="tray.error">{{ tray.error }}</div>
</div>`,
  };
})();
