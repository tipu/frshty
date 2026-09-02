(function () {
	'use strict';

	// Stored timestamps are UTC. Every render layer converts to the zone the
	// browser is in, so the page shows the reader's wall clock.
	const INSTANT = /^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:?\d{2})$/;

	function toDate(value) {
		if (value === null || value === undefined || value === '') return null;
		const d = value instanceof Date ? value : new Date(value);
		return isNaN(d.getTime()) ? null : d;
	}

	function isInstant(value) {
		return typeof value === 'string' && INSTANT.test(value.trim());
	}

	const pad = n => String(n).padStart(2, '0');

	function clock(value) {
		const d = toDate(value);
		if (!d) return '';
		return `${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
	}

	function day(value) {
		const d = toDate(value);
		if (!d) return '';
		return `${pad(d.getMonth() + 1)}/${pad(d.getDate())}`;
	}

	function dayClock(value) {
		const d = toDate(value);
		if (!d) return '';
		return `${day(d)} ${clock(d)}`;
	}

	function dayMinute(value) {
		const d = toDate(value);
		if (!d) return '';
		return `${day(d)} ${pad(d.getHours())}:${pad(d.getMinutes())}`;
	}

	function dateOnly(value) {
		const d = toDate(value);
		if (!d) return '';
		return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}`;
	}

	function stamp(value) {
		const d = toDate(value);
		if (!d) return '';
		return `${dateOnly(d)} ${clock(d)}`;
	}

	function dateTime(value, options) {
		const d = toDate(value);
		if (!d) return '';
		return d.toLocaleString(undefined, options || {
			month: 'short', day: 'numeric', year: 'numeric',
			hour: 'numeric', minute: '2-digit',
		});
	}

	function relTime(value) {
		const d = toDate(value);
		if (!d) return '';
		const secs = Math.floor((Date.now() - d.getTime()) / 1000);
		if (secs < 60) return 'just now';
		const mins = Math.floor(secs / 60);
		if (mins < 60) return `${mins}m ago`;
		const hrs = Math.floor(mins / 60);
		if (hrs < 24) return `${hrs}h ago`;
		return `${Math.floor(hrs / 24)}d ago`;
	}

	// Values that arrive inside generic label/value rows: format the ones that
	// are timestamps, pass everything else through untouched.
	function localize(value, formatter) {
		if (!isInstant(value)) return value;
		return (formatter || stamp)(value);
	}

	window.frshtyTime = {
		toDate, isInstant, clock, day, dayClock, dayMinute,
		dateOnly, stamp, dateTime, relTime, localize,
	};
})();
