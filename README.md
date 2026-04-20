# frshty (فرشتہ)

**/fəˈrɪʃ.tə/** — a personal dev dashboard that ties together PR reviews, tickets, Slack, and timesheets into a single FastAPI interface. Built around AI-assisted code review using Claude, Codex, and Gemini CLIs running inside a dev container.

This is personal automation shared publicly so others can fork and adapt it. It is not a production service.

## Setup

Requires Python 3.12+ and [uv](https://github.com/astral-sh/uv).

```
uv sync
cp config/example.toml config/local.toml   # edit with your values
python frshty.py config/local.toml
```

Set credentials via environment variables referenced in your config (e.g. `BB_TOKEN`, `JIRA_TOKEN`, `LINEAR_TOKEN`). See `config/example.toml` for the full list of options.

### Timezone

Set `FRSHTY_TIMEZONE` in your environment (any IANA zone, e.g. `America/Los_Angeles`, `Europe/Paris`, `Asia/Tokyo`). Default is `UTC`.

```
FRSHTY_TIMEZONE=America/Los_Angeles
```

Single source of truth for calendar-day semantics: "today", the timesheet fill window, billing fire times (Fri 7pm local, last-day-of-month 7pm local), recurring meeting weekday matching. Stored timestamps in sqlite, logs, and events are always UTC; rendering converts to this zone for display.

### Slack

Slack functionality requires [slack-proxy-tools](https://github.com/tipu/slack-proxy-tools) to be checked out and running. Follow the setup instructions in that repo to get the proxy server configured before enabling Slack features in your config.

## Docker

The included Dockerfile builds a dev container with Claude Code, Codex, and Gemini CLI pre-installed. This is intentional — the container is designed for AI-assisted code review workflows, not as a minimal runtime image.

```
cp docker-compose.example.yml docker-compose.yml  # edit paths
docker compose up
```

## Security

The app binds to `127.0.0.1` by default. Endpoints are unauthenticated. Do not expose to a network without adding your own auth layer.

## License

MIT
