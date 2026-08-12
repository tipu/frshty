(() => {
	const STYLE_ID = 'frshty-click-indicator-style'
	const CSS = `
@keyframes frshty-ring {
	0%   { transform: translate(-50%, -50%) scale(0.25); opacity: 1; }
	100% { transform: translate(-50%, -50%) scale(1);    opacity: 0; }
}
@keyframes frshty-dot {
	0%   { transform: translate(-50%, -50%) scale(1);   opacity: 0.95; }
	100% { transform: translate(-50%, -50%) scale(0.4); opacity: 0; }
}
.frshty-click-ring, .frshty-click-dot {
	position: fixed;
	border-radius: 50%;
	pointer-events: none;
	z-index: 2147483647;
}
.frshty-click-ring {
	width: 84px; height: 84px;
	border: 4px solid rgba(255, 40, 60, 0.95);
	background: rgba(255, 40, 60, 0.14);
	animation: frshty-ring 600ms cubic-bezier(0.22, 0.61, 0.36, 1) forwards;
}
.frshty-click-dot {
	width: 18px; height: 18px;
	background: rgba(255, 40, 60, 0.95);
	animation: frshty-dot 600ms ease-out forwards;
}
`

	const ensureStyle = () => {
		if (document.getElementById(STYLE_ID)) return
		const host = document.head || document.documentElement
		if (!host) return
		const el = document.createElement('style')
		el.id = STYLE_ID
		el.textContent = CSS
		host.appendChild(el)
	}

	const mark = (x, y) => {
		ensureStyle()
		const host = document.body || document.documentElement
		if (!host) return
		for (const cls of ['frshty-click-ring', 'frshty-click-dot']) {
			const node = document.createElement('div')
			node.className = cls
			node.style.left = `${x}px`
			node.style.top = `${y}px`
			host.appendChild(node)
			setTimeout(() => node.remove(), 700)
		}
	}

	document.addEventListener('pointerdown', e => mark(e.clientX, e.clientY), true)
})()
