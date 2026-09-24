FROM python:3.12-slim

ARG HOST_UID=1000
ARG HOST_GID=1000
ARG HOST_HOME=/home/frshty
ARG HOOK_DIR=

ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV NODE_PATH=/usr/lib/node_modules

RUN apt-get update && apt-get install -y curl git openssh-client tmux libsecret-1-0 tree procps sqlite3 && \
    curl -fsSL https://deb.nodesource.com/setup_22.x | bash - && \
    apt-get install -y nodejs && \
    curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" > /etc/apt/sources.list.d/github-cli.list && \
    apt-get update && apt-get install -y gh && \
    npm install -g @anthropic-ai/claude-code @openai/codex pnpm playwright && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install --no-cache-dir httpx fastapi 'uvicorn[standard]' watchfiles mcp playwright pytest uv && \
    python -m playwright install --with-deps chromium && \
    chmod -R a+rX /ms-playwright

RUN groupadd -o -g "${HOST_GID}" frshty && \
    useradd -o -m -d "${HOST_HOME}" -s /bin/bash -u "${HOST_UID}" -g "${HOST_GID}" frshty && \
    mkdir -p /run/frshty/seed && \
    if [ -n "${HOOK_DIR}" ]; then mkdir -p "$(dirname "${HOOK_DIR}")" && ln -s /app "${HOOK_DIR}"; fi

COPY . .

USER frshty
ENV HOME=${HOST_HOME}
ENTRYPOINT ["python", "/app/scripts/container_boot.py"]
