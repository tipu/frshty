import os, json, re
from collections import Counter
from pathlib import Path

projects_dir = Path('/home/tipu/.claude/projects')

all_jsonl = []
for p in projects_dir.rglob('*.jsonl'):
    try:
        all_jsonl.append((p.stat().st_mtime, p))
    except:
        pass

all_jsonl.sort(reverse=True)
recent = [p for _, p in all_jsonl[:50]]

bash_cmds = Counter()
mcp_tools = Counter()

def leading_token(cmd):
    cmd = cmd.strip()
    tokens = cmd.split()
    i = 0
    while i < len(tokens) and '=' in tokens[i] and not tokens[i].startswith('-'):
        i += 1
    if i >= len(tokens):
        return ''
    first = tokens[i]
    if first in ('sudo', 'timeout') and i+1 < len(tokens):
        i += 1
        first = tokens[i]
    return first

def cmd_key(cmd):
    parts = re.split(r'[;&|]', cmd)
    first = parts[0].strip()
    tok = leading_token(first)
    if not tok:
        return ''
    tokens = first.split()
    try:
        idx = next(i for i, t in enumerate(tokens) if t == tok)
    except:
        idx = 0
    if tok in ('git', 'gh', 'docker', 'kubectl', 'npm', 'yarn', 'pnpm', 'bun', 'systemctl', 'launchctl', 'curl', 'psql') and idx+1 < len(tokens):
        sub = tokens[idx+1]
        if not sub.startswith('-'):
            return tok + ' ' + sub
    return tok

for jsonl_path in recent:
    try:
        with open(jsonl_path) as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    if obj.get('type') != 'assistant':
                        continue
                    msg = obj.get('message', {})
                    for item in msg.get('content', []):
                        if item.get('type') != 'tool_use':
                            continue
                        name = item.get('name', '')
                        inp = item.get('input', {})
                        if name == 'Bash':
                            cmd = inp.get('command', '')
                            key = cmd_key(cmd)
                            if key:
                                bash_cmds[key] += 1
                        elif name.startswith('mcp__'):
                            mcp_tools[name] += 1
                except:
                    pass
    except:
        pass

print('BASH COMMANDS')
for cmd, count in bash_cmds.most_common(40):
    print(f'{count:4d}  {cmd}')

print()
print('MCP TOOLS')
for tool, count in mcp_tools.most_common(20):
    print(f'{count:4d}  {tool}')
