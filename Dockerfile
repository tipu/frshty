FROM python:3.12-slim

ARG HOST_UID=1000
ARG HOST_GID=1000
ARG HOST_HOME=/home/frshty
ARG HOOK_DIR=

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV NODE_PATH=/usr/lib/node_modules
ENV PLAYWRIGHT_MCP_SANDBOX=false

RUN apt-get update && apt-get install -y curl git git-lfs openssh-client tmux libsecret-1-0 tree procps sqlite3 vim \
        build-essential gnupg jq ripgrep rsync ffmpeg xvfb postgresql-client default-libmysqlclient-dev pkg-config && \
    curl -fsSL https://deb.nodesource.com/setup_22.x | bash - && \
    apt-get install -y nodejs && \
    curl -fsSL https://cli.github.com/packages/githubcli-archive-keyring.gpg | dd of=/usr/share/keyrings/githubcli-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/githubcli-archive-keyring.gpg] https://cli.github.com/packages stable main" > /etc/apt/sources.list.d/github-cli.list && \
    curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker.gpg] https://download.docker.com/linux/debian $(. /etc/os-release && echo $VERSION_CODENAME) stable" > /etc/apt/sources.list.d/docker.list && \
    apt-get update && apt-get install -y gh docker-ce-cli docker-compose-plugin && \
    npm install -g @anthropic-ai/claude-code @openai/codex pnpm playwright @playwright/cli pyright typescript-language-server typescript@5 && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y unzip oathtool && \
    curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip" -o /tmp/awscliv2.zip && \
    unzip -q /tmp/awscliv2.zip -d /tmp && /tmp/aws/install && \
    curl -fsSL "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/ubuntu_$(dpkg --print-architecture | sed 's/amd64/64bit/')/session-manager-plugin.deb" -o /tmp/ssm.deb && \
    dpkg -i /tmp/ssm.deb && \
    rm -rf /tmp/aws /tmp/awscliv2.zip /tmp/ssm.deb && apt-get clean && rm -rf /var/lib/apt/lists/*

RUN case "$(dpkg --print-architecture)" in \
        amd64) arch=x64; sum=551f6fc83ea457d62a0d98237cbad105af8d557003051f41f3e7ca7b3f2470eb ;; \
        arm64) arch=arm64; sum=e4a487ee7ccd7d3a7f7ec08657610aa3606637dab924210b3aee62570fb4b080 ;; \
        *) echo "gitleaks: unsupported architecture" >&2; exit 1 ;; \
    esac && \
    curl -fsSL "https://github.com/gitleaks/gitleaks/releases/download/v8.30.1/gitleaks_8.30.1_linux_${arch}.tar.gz" -o /tmp/gitleaks.tar.gz && \
    echo "${sum}  /tmp/gitleaks.tar.gz" | sha256sum -c - && \
    tar -xzf /tmp/gitleaks.tar.gz -C /usr/local/bin gitleaks && \
    rm /tmp/gitleaks.tar.gz && gitleaks version

WORKDIR /app

RUN pip install --no-cache-dir httpx fastapi 'uvicorn[standard]' watchfiles jsonschema mcp playwright pytest uv pipenv pre-commit && \
    python -m playwright install --with-deps chromium && \
    chmod -R a+rX /ms-playwright

RUN if [ "$(dpkg --print-architecture)" = "amd64" ]; then python -m playwright install --with-deps chrome; fi && \
    npm install -g github:tipu/google-voice-cli#be58221df19229e9f3a5cf2cf4eb9f862235fc5b && \
    node "$(npm root -g)/google-voice-cli/node_modules/playwright-core/cli.js" install chromium && \
    chmod -R a+rX /ms-playwright && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN groupadd -o -g "${HOST_GID}" frshty && \
    useradd -o -m -d "${HOST_HOME}" -s /bin/bash -u "${HOST_UID}" -g "${HOST_GID}" frshty && \
    install -d -o frshty -g frshty "${HOST_HOME}/.config" "${HOST_HOME}/.local" "${HOST_HOME}/.local/share" "${HOST_HOME}/.local/state" && \
    mkdir -p /run/frshty/seed /etc/codex && \
    printf 'sandbox_mode = "danger-full-access"\napproval_policy = "never"\n' > /etc/codex/config.toml && \
    ln -s /app/scripts/totp /usr/local/bin/totp && \
    ln -s /app/scripts/secret.py /usr/local/bin/secret && \
    echo '[ -f /app/scripts/shell_aliases ] && . /app/scripts/shell_aliases' >> /etc/bash.bashrc && \
    if [ -n "${HOOK_DIR}" ]; then mkdir -p "$(dirname "${HOOK_DIR}")" && ln -s /app "${HOOK_DIR}"; fi

COPY . .

USER frshty
ENV HOME=${HOST_HOME}
ENTRYPOINT ["python", "/app/scripts/container_boot.py"]
