UPDATE work_items SET proposal_key = 'deliver:' || substr(proposal_key, instr(proposal_key, ':') + 1)
WHERE substr(proposal_key, 1, instr(proposal_key, ':') - 1) IN ('commit', 'push', 'pr', 'merge', 'work');
