FROM python:3.12-slim

ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV NODE_PATH=/usr/lib/node_modules

RUN apt-get update && apt-get install -y curl git tmux sudo libsecret-1-0 tree procps && \
    curl -fsSL https://deb.nodesource.com/setup_22.x | bash - && \
    apt-get install -y nodejs && \
    curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" > /etc/apt/sources.list.d/github-cli.list && \
    apt-get update && apt-get install -y gh && \
    npm install -g @anthropic-ai/claude-code @openai/codex @google/gemini-cli playwright && \
    npx playwright install-deps chromium && \
    npx playwright install chromium && \
    chmod -R a+rX /ms-playwright && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /app

RUN pip install fastapi 'uvicorn[standard]' httpx watchfiles

RUN groupadd -g 986 dockerhost || true && \
    useradd -m -s /bin/bash -u 1000 claude && \
    usermod -aG dockerhost claude && \
    echo "claude ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/claude && \
    git config --global --add safe.directory '*' && \
    sudo -u claude git config --global --add safe.directory '*' && \
    echo 'alias cl="claude --dangerously-skip-permissions"' >> /home/claude/.bashrc

COPY . .

USER claude
COPY config/ssh_config /etc/ssh/frshty_ssh_config
ENV GIT_SSH_COMMAND="ssh -F /etc/ssh/frshty_ssh_config"
CMD ["python", "frshty.py", "config/example.toml"]
