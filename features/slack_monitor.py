import json
import re
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
from pathlib import Path

import core.log as log
import core.slack_capture as slack_capture
import core.state as state
from core.claude_runner import run_haiku, extract_json


def _msg_ts_iso(record: dict) -> str:
    """Slack message wall-clock time in ISO. Uses payload.ts (authoritative
    slack timestamp) because slack_int's record `dt` is when the line was
    written, which for REST history pulls can be today even if the message
    is weeks old."""
    payload = record.get("payload", {})
    if isinstance(payload, dict):
        raw = payload.get("ts", "")
        if raw:
            try:
                return datetime.fromtimestamp(float(raw), tz=timezone.utc).isoformat()
            except (ValueError, TypeError):
                pass
    return record.get("dt", "")


def check(config: dict):
    slack_cfg = config.get("slack", {})
    messages_dir = str(slack_cfg.get("messages_dir") or "")
    raw_path = str(slack_cfg.get("raw_path") or "")
    capture = slack_capture.live_path(messages_dir, raw_path)
    if not capture or not Path(capture).exists():
        return

    workspace = slack_cfg.get("workspace", "")
    base_url = config["_base_url"]
    max_age_hours = int(slack_cfg.get("mention_max_age_hours", 48))
    # Don't emit attention events for messages older than this. slack_int can
    # REST-sync days-old conversations at any time; those shouldn't spam the
    # feed even if they sit past the high-water mark.
    notify_max_age_hours = int(slack_cfg.get("notify_max_age_hours", 2))
    notify_cutoff_iso = (datetime.now(timezone.utc) - timedelta(hours=notify_max_age_hours)).isoformat()

    sl = state.load("slack")
    # The configured id wins over the discovered one. Discovery reads the boot
    # payloads, and the filtered capture drops them because they carry no
    # message, so on a filtered capture config is the only source there is.
    user_id = str(slack_cfg.get("user_id") or "").strip() or sl.get("user_id", "")
    team_id = str(slack_cfg.get("team_id") or "").strip() or sl.get("team_id", "")
    name = Path(capture).name
    offsets = _offsets(sl)
    offset = offsets.get(name, 0)
    last_dt = sl.get("last_dt", "")

    file_size = Path(capture).stat().st_size
    rotated = offset > file_size
    if rotated:
        offset = 0

    # First run on this instance: skip the backlog. Seed last_dt to now so only
    # messages that land after this point are ever considered for attention.
    if not last_dt:
        now_iso = datetime.now(timezone.utc).isoformat()
        sl["last_dt"] = now_iso
        offsets[name] = file_size
        sl["offsets"] = offsets
        state.save("slack", sl)
        return

    with open(capture) as f:
        f.seek(offset)
        new_lines = f.readlines()
        new_offset = f.tell()

    if not new_lines:
        offsets[name] = new_offset
        sl["offsets"] = offsets
        state.save("slack", sl)
        return

    names = sl.get("names", {})
    for uid, display in slack_capture.user_names(messages_dir, raw_path).items():
        names.setdefault(uid, display)
    messages = []
    # A message can reach the capture twice, once live and once in a REST
    # history pull. The raw log hid the second copy, because a REST batch has
    # no text on the record itself and this scan reads one message per record.
    # The filtered log unpacks the batch, so the copy is a record like any
    # other and would raise a second attention event for one message.
    seen = set()
    for line in new_lines:
        try:
            record = json.loads(line.strip())
        except json.JSONDecodeError:
            continue
        if slack_capture.is_filtered_record(record):
            record = slack_capture.as_capture_record(record)
            if record is None:
                continue
        # High-water mark against the SLACK message time, not slack_int's record
        # time — REST history pulls land with dt=now but payload.ts = weeks ago.
        if last_dt and _msg_ts_iso(record) <= last_dt:
            continue
        if workspace and not _matches_workspace(record, workspace, team_id):
            continue
        if not user_id:
            user_id = _extract_user_id(record, workspace)
            if user_id:
                sl["user_id"] = user_id
        if not team_id:
            team_id = _extract_team_id(record, workspace)
            if team_id:
                sl["team_id"] = team_id
        _collect_names(record, names)
        text = _extract_text(record)
        if text:
            key = (_extract_channel(record),
                   record.get("payload", {}).get("ts", ""), text)
            if key in seen:
                continue
            seen.add(key)
        messages.append(record)
    sl["names"] = names
    _warn_when_the_operator_has_no_id(base_url, sl, user_id)

    mentions = [m for m in messages if _is_mention(m, user_id) or _is_dm_to_me(m, user_id)]
    # Only emit + triage for mentions young enough to be actionable. Older ones
    # still clear the high-water mark so they won't re-surface, but don't spam.
    mentions = [m for m in mentions if _msg_ts_iso(m) > notify_cutoff_iso]
    for mention in mentions:
        text = _resolve_names(_extract_text(mention), names)
        if not text:
            continue
        raw_channel = _extract_channel(mention)
        channel = names.get(raw_channel, raw_channel)
        if raw_channel.startswith("D"):
            sender = mention.get("payload", {}).get("user", "")
            channel = f"DM:{names.get(sender, sender)}"
        elif not channel.startswith("#"):
            channel = f"#{channel}"

        surrounding = _gather_surrounding(mention, messages, names)
        work_context = _gather_context(config)

        triage = run_haiku(
            f"Classify this Slack message. Reply with exactly one word: REPLY, REACT, or IGNORE.\n"
            f"REPLY = direct question, request for info, needs a substantive text response\n"
            f"REACT = good news, acknowledgment, FYI — a thumbs up reaction is sufficient\n"
            f"IGNORE = automated message, bot noise, no response needed\n\n"
            f"Context:\n{surrounding}\n\nMessage: {text}"
        )
        action = "reply"
        if triage:
            t = triage.strip().upper()
            if "REACT" in t:
                action = "react"
            elif "IGNORE" in t:
                action = "ignore"

        if action == "ignore":
            continue

        suggested = ""
        if action == "reply":
            suggested = run_haiku(
                f"Suggest a short slack reply to this message. Match this style exactly:\n"
                f"- all lowercase, no capitalization\n"
                f"- very direct and to the point, few words\n"
                f"- for positive news, just say 'nice' or similar\n"
                f"- occasionally end with 'lol' but don't overdo it\n"
                f"- no formality, no greetings, no sign-offs\n"
                f"- if the message asks about work context (PRs, tickets, code), use the context below to give a substantive answer in the same casual style\n"
                f"Return ONLY the reply text, nothing else.\n\n"
                f"Surrounding messages:\n{surrounding}\n\nMessage to reply to: {text}\n\n{work_context}"
            ) or ""
        elif action == "react":
            suggested = "+1"

        payload = mention.get("payload", {})
        reply_id = payload.get("ts", "")
        reply_ctx = {
            "channel": payload.get("channel", ""),
            "thread_ts": payload.get("thread_ts", reply_id),
            "workspace": workspace,
        }
        existing_replies = sl.get("replies", {})
        existing_replies[reply_id] = reply_ctx
        sl["replies"] = existing_replies

        log.emit("slack_mention_detected", f"{'DM' if raw_channel.startswith('D') else '@mention'} in {channel}: {text[:80]}",
            links={"detail": f"{base_url}/slack"},
            meta={"channel": channel, "text": text[:200], "suggested_response": suggested, "action": action, "reply_id": reply_id})

    thread_msgs = [m for m in messages if _is_in_thread(m, user_id)]
    thread_msgs = [m for m in thread_msgs if _msg_ts_iso(m) > notify_cutoff_iso]
    for msg in thread_msgs:
        text = _extract_text(msg)
        if not text:
            continue
        classification = run_haiku(
            f"Is this Slack thread message actionable for me (needs my response or action)? "
            f"Reply JSON: {{\"actionable\": true/false, \"reason\": \"brief\"}}\n\n{text}"
        )
        parsed = extract_json(classification) if classification else None
        if isinstance(parsed, dict) and parsed.get("actionable") is True:
            log.emit("slack_actionable_item", f"Actionable thread message: {text[:80]}",
                links={"detail": f"{base_url}/slack"},
                meta={"text": text[:200]})

    _resolve_channel_names(config, names)

    channel_digests = sl.get("channel_digests", {})
    if messages:
        by_channel: dict[str, list[str]] = {}
        for m in messages:
            ch_id = _extract_channel(m)
            text = _extract_text(m)
            # A message pulled over REST names no channel, so there is no
            # channel to digest it under. The raw log never reached here with
            # one, because a REST batch carries no text on the record itself.
            if not text or not ch_id or ch_id.startswith("D"):
                continue
            ch_name = names.get(ch_id, ch_id)
            if not ch_name.startswith("#"):
                ch_name = f"#{ch_name}"
            by_channel.setdefault(ch_name, []).append(text)

        for ch_name, texts in by_channel.items():
            existing = channel_digests.get(ch_name, {})
            prev_count = existing.get("message_count", 0)
            all_text = "\n".join(texts[-30:])
            prev_summary = existing.get("summary", "")
            prompt = (
                f"Summarize what's happening in the Slack channel {ch_name}. "
                f"Be concise — 1-3 sentences max. Focus on decisions, asks, and status changes. Skip chatter.\n\n"
            )
            if prev_summary:
                prompt += f"Previous summary: {prev_summary}\n\nNew messages:\n{all_text[:3000]}"
            else:
                prompt += f"Messages:\n{all_text[:3000]}"
            summary = run_haiku(prompt)
            if summary:
                channel_digests[ch_name] = {
                    "summary": summary.strip(),
                    "message_count": prev_count + len(texts),
                    "new_messages": len(texts),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
    sl["channel_digests"] = channel_digests

    offsets[name] = new_offset
    sl["offsets"] = offsets
    if messages:
        last_record_dt = max(_msg_ts_iso(m) for m in messages)
        if last_record_dt:
            sl["last_dt"] = max(sl.get("last_dt", ""), last_record_dt)

    # Age out stored mentions so the UI doesn't keep surfacing stale items.
    cutoff_iso = (datetime.now(timezone.utc) - timedelta(hours=max_age_hours)).isoformat()
    existing_mentions = [m for m in sl.get("mentions", []) if m.get("ts", "") > cutoff_iso]
    for mention in mentions:
        text = _resolve_names(_extract_text(mention), names)
        channel = names.get(_extract_channel(mention), _extract_channel(mention))
        # Store the slack message time, not the processing time, so age reflects reality.
        msg_dt = _msg_ts_iso(mention) or datetime.now(timezone.utc).isoformat()
        existing_mentions.append({
            "text": text[:500] if text else "",
            "channel": channel,
            "ts": msg_dt,
        })
    sl["mentions"] = existing_mentions[-50:]

    state.save("slack", sl)


def _offsets(sl: dict) -> dict:
    """Where the last scan stopped in each capture file it read.

    The position is keyed by file name because there is more than one capture
    file to read now: filtered.jsonl when slack_int has written one, and
    messages.jsonl when it has not. One shared position would be applied to
    whichever file this scan opens, and a position taken in a 160 MB raw log
    would put a scan of a 1 MB filtered log past its end. An older state file
    holds one bare position and no name; it is read as the raw log's."""
    offsets = sl.get("offsets")
    if not isinstance(offsets, dict):
        offsets = {}
        legacy = sl.get("file_offset")
        if isinstance(legacy, int):
            offsets[slack_capture.RAW_FILE] = legacy
    return {str(k): v for k, v in offsets.items() if isinstance(v, int)}


def _warn_when_the_operator_has_no_id(base_url: str, sl: dict, user_id: str) -> None:
    """Say so once when nothing can name the operator.

    Without an id no message is a mention and no message is a direct message,
    so this scan reads the whole capture and raises nothing, silently. The
    filtered capture makes that reachable: discovery read the boot payloads
    and the filter drops them, so a capture that has never been scanned raw
    has no id to carry forward and `[slack] user_id` has to supply it."""
    if user_id:
        sl.pop("user_id_missing", None)
        return
    if sl.get("user_id_missing"):
        return
    sl["user_id_missing"] = True
    log.emit("slack_user_id_missing",
             "slack: no user id for this workspace, so no mention or direct "
             "message is detected; set [slack] user_id",
             links={"detail": f"{base_url}/slack"})


def _gather_surrounding(mention: dict, messages: list, names: dict) -> str:
    payload = mention.get("payload", {})
    channel = payload.get("channel", "")
    thread_ts = payload.get("thread_ts", "")
    mention_ts = payload.get("ts", "")

    prior = []
    for m in messages:
        p = m.get("payload", {})
        if not isinstance(p, dict) or p.get("type") != "message":
            continue
        m_ch = p.get("channel", "")
        m_ts = p.get("ts", "")
        m_thread = p.get("thread_ts", "")

        if thread_ts and m_thread == thread_ts and m_ch == channel and m_ts < mention_ts:
            sender = names.get(p.get("user", ""), p.get("user", ""))
            text = _resolve_names(p.get("text", ""), names)
            prior.append(f"{sender}: {text[:200]}")
        elif not thread_ts and m_ch == channel and m_ts < mention_ts:
            sender = names.get(p.get("user", ""), p.get("user", ""))
            text = _resolve_names(p.get("text", ""), names)
            prior.append(f"{sender}: {text[:200]}")

    return "\n".join(prior[-10:])


def _gather_context(config: dict) -> str:
    parts = []
    ticket_state = state.load("tickets")
    if ticket_state:
        items = []
        for key, ts in ticket_state.items():
            if isinstance(ts, dict):
                items.append(f"  {key}: status={ts.get('status','')}, branch={ts.get('branch','')}")
        if items:
            parts.append("Recent tickets:\n" + "\n".join(items[-10:]))

    events = log.get_events(limit=30)
    recent = []
    for e in events:
        if e["event"] in ("ticket_pr_created", "ticket_implemented", "ticket_plan_created", "review_complete"):
            recent.append(f"  {e['event']}: {e['summary']}")
    if recent:
        parts.append("Recent activity:\n" + "\n".join(recent[-10:]))

    if not parts:
        return ""
    return "Work context:\n" + "\n".join(parts)


def _collect_names(record: dict, names: dict):
    payload = record.get("payload", {})
    if not isinstance(payload, dict):
        return
    self_data = payload.get("self", {})
    if isinstance(self_data, dict) and self_data.get("id"):
        names[self_data["id"]] = self_data.get("real_name") or self_data.get("name", "")
    for ch in payload.get("channels", []):
        if not isinstance(ch, dict):
            continue
        ch_id = ch.get("id") or ch.get("channel_id")
        ch_name = ch.get("name") or ch.get("name_normalized")
        if ch_id and ch_name:
            names[ch_id] = f"#{ch_name}"
    for u in payload.get("users", []):
        if isinstance(u, dict) and u.get("id"):
            names[u["id"]] = u.get("real_name") or u.get("name", "")
    if payload.get("type") == "message":
        uid = payload.get("user", "")
        if uid and uid not in names:
            profile = payload.get("user_profile", {})
            if isinstance(profile, dict):
                name = profile.get("real_name") or profile.get("display_name") or profile.get("name")
                if name:
                    names[uid] = name
    for block in payload.get("blocks", []):
        if not isinstance(block, dict):
            continue
        for element in block.get("elements", []):
            if not isinstance(element, dict):
                continue
            for item in element.get("elements", []):
                if not isinstance(item, dict):
                    continue
                if item.get("type") == "user" and item.get("user_id"):
                    uid = item["user_id"]
                    if uid not in names and item.get("name"):
                        names[uid] = item["name"]


def _resolve_names(text: str, names: dict) -> str:
    def _replace(m):
        prefix, slack_id = m.group(1), m.group(2)
        name = names.get(slack_id)
        if not name:
            return m.group(0)
        if prefix == "@":
            return f"@{name}"
        return name if name.startswith("#") else f"#{name}"
    return re.sub(r"<([@#])([A-Z0-9]+)>", _replace, text)


def _extract_user_id(record: dict, workspace: str) -> str:
    endpoint = record.get("endpoint", "")
    if "userBoot" not in endpoint and "auth.findUser" not in endpoint:
        return ""
    if workspace and workspace not in endpoint:
        return ""
    payload = record.get("payload", {})
    self_data = payload.get("self", {})
    return self_data.get("id", "")


def _matches_workspace(record: dict, workspace: str, team_id: str = "") -> bool:
    # A filtered record names its workspace outright, which is what the team
    # id and the endpoint below are read to work out for a raw one.
    named = str(record.get("workspace") or "")
    if named:
        return named == workspace
    if record.get("source") == "ws":
        if not team_id:
            return True
        payload = record.get("payload", {})
        if isinstance(payload, dict):
            return payload.get("team", "") == team_id
        return False
    endpoint = record.get("endpoint", "")
    return workspace in endpoint


def _extract_team_id(record: dict, workspace: str) -> str:
    endpoint = record.get("endpoint", "")
    if "userBoot" not in endpoint:
        return ""
    if workspace and workspace not in endpoint:
        return ""
    payload = record.get("payload", {})
    if not isinstance(payload, dict):
        return ""
    team = payload.get("team", {})
    if isinstance(team, dict):
        return team.get("id", "")
    return ""


def _is_mention(record: dict, user_id: str) -> bool:
    if not user_id:
        return False
    payload = record.get("payload", {})
    if not isinstance(payload, dict):
        return False
    text = payload.get("text", "")
    if f"<@{user_id}>" in text:
        return True
    if "<!here>" in text or "<!channel>" in text:
        return True
    for block in payload.get("blocks", []):
        if not isinstance(block, dict):
            continue
        for element in block.get("elements", []):
            if not isinstance(element, dict):
                continue
            for item in element.get("elements", []):
                if not isinstance(item, dict):
                    continue
                if item.get("type") == "user" and item.get("user_id") == user_id:
                    return True
                if item.get("type") == "broadcast":
                    return True
    return False


def _is_dm_to_me(record: dict, user_id: str) -> bool:
    if not user_id:
        return False
    payload = record.get("payload", {})
    if not isinstance(payload, dict):
        return False
    channel = payload.get("channel", "")
    if not isinstance(channel, str):
        return False
    sender = payload.get("user", "")
    if channel.startswith("D") and sender and sender != user_id:
        return True
    return False


def _is_in_thread(record: dict, _user_id: str) -> bool:
    payload = record.get("payload", {})
    return bool(payload.get("thread_ts")) and payload.get("thread_ts") != payload.get("ts")


def _extract_text(record: dict) -> str:
    payload = record.get("payload", {})
    return payload.get("text", "")


def _extract_channel(record: dict) -> str:
    payload = record.get("payload", {})
    return payload.get("channel", "unknown")


_last_channel_resolve = ""


def _resolve_channel_names(config: dict, names: dict):
    global _last_channel_resolve
    now = datetime.now(timezone.utc).isoformat()
    if _last_channel_resolve and (datetime.fromisoformat(now) - datetime.fromisoformat(_last_channel_resolve)).total_seconds() < 3600:
        return

    slack_cfg = config.get("slack", {})
    folder = slack_capture.capture_dir(str(slack_cfg.get("messages_dir") or ""),
                                       str(slack_cfg.get("raw_path") or ""))
    if not folder:
        return
    tokens_file = str(Path(folder).parent / "tokens.json")
    workspace = slack_cfg.get("workspace", "")
    try:
        tokens = json.loads(Path(tokens_file).read_text())
        creds = tokens.get(workspace, {})
        if not creds.get("token"):
            return
        data = urllib.parse.urlencode({"token": creds["token"], "types": "public_channel,private_channel,mpim", "limit": "1000"}).encode()
        req = urllib.request.Request(
            f"https://{workspace}.slack.com/api/conversations.list",
            data,
            headers={"Cookie": creds["cookie"].replace(", ", "; ")},
        )
        resp = urllib.request.urlopen(req, timeout=30)
        result = json.loads(resp.read())
        if result.get("ok"):
            for ch in result.get("channels", []):
                if ch.get("id") and ch.get("name"):
                    names[ch["id"]] = f"#{ch['name']}"

        data2 = urllib.parse.urlencode({"token": creds["token"], "limit": "200"}).encode()
        req2 = urllib.request.Request(
            f"https://{workspace}.slack.com/api/users.list",
            data2,
            headers={"Cookie": creds["cookie"].replace(", ", "; ")},
        )
        resp2 = urllib.request.urlopen(req2, timeout=30)
        result2 = json.loads(resp2.read())
        if result2.get("ok"):
            for u in result2.get("members", []):
                if u.get("id") and not u.get("deleted"):
                    names[u["id"]] = u.get("real_name") or u.get("name", "")

        _last_channel_resolve = now
    except Exception:
        pass


def _hours_since(iso_ts: str) -> float:
    try:
        dt = datetime.fromisoformat(iso_ts)
        return (datetime.now(timezone.utc) - dt).total_seconds() / 3600
    except (ValueError, TypeError):
        return 999
