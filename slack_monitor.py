import json
from datetime import datetime, timezone
from pathlib import Path

import log
import state
from claude_runner import run_haiku


def check(config: dict):
    slack_cfg = config.get("slack", {})
    raw_path = slack_cfg.get("raw_path")
    if not raw_path or not Path(raw_path).exists():
        return

    workspace = slack_cfg.get("workspace", "")
    base_url = config["_base_url"]

    sl = state.load("slack")
    user_id = sl.get("user_id", "")
    team_id = sl.get("team_id", "")
    offset = sl.get("file_offset", 0)

    with open(raw_path) as f:
        f.seek(offset)
        new_lines = f.readlines()
        new_offset = f.tell()

    if not new_lines:
        sl["file_offset"] = new_offset
        state.save("slack", sl)
        return

    names = sl.get("names", {})
    messages = []
    for line in new_lines:
        try:
            record = json.loads(line.strip())
        except json.JSONDecodeError:
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
        messages.append(record)
    sl["names"] = names

    mentions = [m for m in messages if _is_mention(m, user_id) or _is_dm_to_me(m, user_id)]
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
    for msg in thread_msgs:
        text = _extract_text(msg)
        if not text:
            continue
        classification = run_haiku(
            f"Is this Slack thread message actionable for me (needs my response or action)? "
            f"Reply JSON: {{\"actionable\": true/false, \"reason\": \"brief\"}}\n\n{text}"
        )
        if classification and '"actionable": true' in classification.lower():
            log.emit("slack_actionable_item", f"Actionable thread message: {text[:80]}",
                links={"detail": f"{base_url}/slack"},
                meta={"text": text[:200]})

    last_digest = sl.get("last_digest_at", "")
    if not last_digest or _hours_since(last_digest) >= 4:
        if messages:
            all_text = "\n".join(_extract_text(m) for m in messages[-50:] if _extract_text(m))
            if all_text:
                digest = run_haiku(
                    f"Summarize these Slack messages into a brief digest. "
                    f"Group by topic. Highlight anything that might need attention.\n\n{all_text[:4000]}"
                )
                if digest:
                    log.emit("slack_digest", digest[:500],
                        links={"detail": f"{base_url}/slack"},
                        meta={"message_count": len(messages)})
            sl["last_digest_at"] = datetime.now(timezone.utc).isoformat()

    sl["file_offset"] = new_offset

    existing_mentions = sl.get("mentions", [])
    for mention in mentions:
        text = _resolve_names(_extract_text(mention), names)
        channel = names.get(_extract_channel(mention), _extract_channel(mention))
        existing_mentions.append({
            "text": text[:500] if text else "",
            "channel": channel,
            "ts": datetime.now(timezone.utc).isoformat(),
        })
    sl["mentions"] = existing_mentions[-50:]

    state.save("slack", sl)


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
        if isinstance(ch, dict) and ch.get("id"):
            names[ch["id"]] = f"#{ch.get('name', '')}"
    for u in payload.get("users", []):
        if isinstance(u, dict) and u.get("id"):
            names[u["id"]] = u.get("real_name") or u.get("name", "")
    if record.get("source") == "ws" and payload.get("type") == "message":
        uid = payload.get("user", "")
        if uid and uid not in names:
            profile = payload.get("user_profile", {})
            if isinstance(profile, dict) and profile.get("real_name"):
                names[uid] = profile["real_name"]


def _resolve_names(text: str, names: dict) -> str:
    import re
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


def _hours_since(iso_ts: str) -> float:
    try:
        dt = datetime.fromisoformat(iso_ts)
        return (datetime.now(timezone.utc) - dt).total_seconds() / 3600
    except (ValueError, TypeError):
        return 999
