#!/usr/bin/env bash
# Attack the mutation harness itself and require it to refuse.
#
# scripts/mutation_check.py is the thing that decides whether a gate is
# guarded. A harness that reports "all gates went red" no matter what it is
# handed is worth nothing, so this script hands it ten shapes it must reject:
# a suite that cannot report, a patch whose anchor is gone, a patch that
# replaces text with itself, an anchor that matches more than one place, a
# named test that does not exist, a real mutation no test kills, a mutant
# whose interpreter dies before pytest can write a report while a stale report
# sits in the tree, a mutant that crashes the test rather than failing its
# assertion, a patch path that escapes the copy, and a selection that names no
# mutation at all. It then hands it one honest mutation and requires a pass,
# because a harness that only ever fails is equally worthless.
set -euo pipefail
cd "$(dirname "$0")/.."
REPO="$(pwd)"
WORK="$(mktemp -d)"
trap 'rm -rf "${WORK}" >/dev/null 2>&1 || true' EXIT

FAILED=0

run_harness() {
  local dir="$1"; shift
  (cd "${dir}" && python3 scripts/mutation_check.py "$@" 2>&1)
}

expect_reject() {
  local label="$1" needle="$2" dir="$3"; shift 3
  local out rc
  set +e
  out="$(run_harness "${dir}" "$@")"
  rc=$?
  set -e
  if [ "${rc}" -eq 0 ]; then
    echo "HARNESS DID NOT NOTICE: ${label}"
    printf '%s\n' "${out}" | tail -3
    FAILED=1
    return
  fi
  if ! printf '%s\n' "${out}" | grep -q -- "${needle}"; then
    echo "HARNESS FAILED FOR THE WRONG REASON: ${label}, wanted ${needle}"
    printf '%s\n' "${out}" | tail -3
    FAILED=1
    return
  fi
  echo "harness rejects: ${label}"
}

expect_accept() {
  local label="$1" dir="$2"; shift 2
  local out rc
  set +e
  out="$(run_harness "${dir}" "$@")"
  rc=$?
  set -e
  if [ "${rc}" -ne 0 ]; then
    echo "HARNESS REFUSED AN HONEST MUTATION: ${label}"
    printf '%s\n' "${out}" | tail -3
    FAILED=1
    return
  fi
  echo "harness accepts: ${label}"
}

stage() {
  python3 - "$1" <<'PY'
import pathlib
import sys

sys.path.insert(0, "scripts")
import mutation_check

mutation_check.copy_repo(pathlib.Path(sys.argv[1]))
PY
}

write_table() {
  local path="$1" target="$2" file="$3" old="$4" new="$5"
  python3 - "${path}" "${target}" "${file}" "${old}" "${new}" <<'PY'
import json
import sys

path, target, source, old, new = sys.argv[1:6]
json.dump([{"label": "attack", "gate": "an attack on the harness",
            "target": target, "path": source, "old": old, "new": new}],
          open(path, "w"))
PY
}

HONEST_TARGET='tests/features/test_defence.py::TestOnlyAnAssertionFailureCounts::test_pytest_exit_5_nothing_collected_is_not_evidence'
ORDINARY_TARGET='tests/core/test_hard_block.py::TestHardBlockEscapesTheRetryExemption::test_an_ordinary_failure_still_retries'

stage "${WORK}/broken_fixture"
python3 - "${WORK}/broken_fixture/tests/conftest.py" <<'PY'
import pathlib
import sys

path = pathlib.Path(sys.argv[1])
text = path.read_text()
old = 'def _retire_kickoff_threads():\n    """'
assert text.count(old) == 1, "the autouse fixture moved; update this attack"
path.write_text(text.replace(
    old,
    'def _retire_kickoff_threads():\n    raise RuntimeError("unrelated fixture failure")\n    """'))
PY
expect_reject "a suite whose fixture cannot run" "CONTROL RUN IS NOT GREEN" \
  "${WORK}/broken_fixture" --only defence_accepts_a_runner_error

write_table "${WORK}/gone.json" "${HONEST_TARGET}" "features/defence.py" \
  "    if exit_code != 41414141:" "    if False:"
expect_reject "a patch whose anchor is gone" "PATCH DID NOT APPLY" \
  "${REPO}" --table "${WORK}/gone.json"

write_table "${WORK}/identical.json" "${HONEST_TARGET}" "features/defence.py" \
  "    if exit_code != 1:" "    if exit_code != 1:"
expect_reject "a patch that replaces text with itself" "PATCH DID NOT APPLY" \
  "${REPO}" --table "${WORK}/identical.json"

write_table "${WORK}/ambiguous.json" "${HONEST_TARGET}" "features/defence.py" \
  "    return False, (" "    return True, ("
expect_reject "an anchor that matches more than one place" "PATCH DID NOT APPLY" \
  "${REPO}" --table "${WORK}/ambiguous.json"

write_table "${WORK}/unknown.json" \
  "tests/features/test_defence.py::TestOnlyAnAssertionFailureCounts::test_no_such_test" \
  "features/defence.py" "    if exit_code != 1:" "    if False:"
expect_reject "a target test that does not exist" "UNKNOWN TARGET TEST" \
  "${REPO}" --table "${WORK}/unknown.json"

write_table "${WORK}/survivor.json" "${ORDINARY_TARGET}" "features/defence.py" \
  "    if exit_code != 1:" "    if False:"
expect_reject "a mutation no named test kills" "MUTATION SURVIVED" \
  "${REPO}" --table "${WORK}/survivor.json"

stage "${WORK}/stale_report"
python3 - "${WORK}/stale_report/.mutation-report.xml" <<'PY'
import pathlib
import sys

pathlib.Path(sys.argv[1]).write_text(
    '<?xml version="1.0" encoding="utf-8"?>\n<testsuites><testsuite><testcase '
    'classname="tests.features.test_defence.TestOnlyAnAssertionFailureCounts" '
    'name="test_pytest_exit_5_nothing_collected_is_not_evidence">'
    '<failure /></testcase></testsuite></testsuites>\n')
PY
write_table "${WORK}/stale.json" "${HONEST_TARGET}" "features/defence.py" \
  "import json
import re" "import os
os._exit(1)
import json
import re"
expect_reject "a mutant whose interpreter dies before pytest can report" \
  "MUTANT SUITE DID NOT RUN" "${WORK}/stale_report" --table "${WORK}/stale.json"

write_table "${WORK}/crash.json" \
  "tests/features/test_work_push_gate.py::TestGatePush::test_lint_failure_denies_before_tests" \
  "services/work_launch.py" \
  '    if lint["status"] not in ("pass", "no_config", "skipped"):' "    if False:"
expect_reject "a mutant that crashes the test instead of failing its assertion" \
  "NOT A KILL" "${REPO}" --table "${WORK}/crash.json"

BEFORE="$(sha256sum features/defence.py | cut -d" " -f1)"
write_table "${WORK}/absolute.json" "${HONEST_TARGET}" "${REPO}/features/defence.py" \
  "    if exit_code != 1:" "    if False:"
expect_reject "a patch path that escapes the copy as an absolute path" \
  "PATCH DID NOT APPLY" "${REPO}" --table "${WORK}/absolute.json"
write_table "${WORK}/traversal.json" "${HONEST_TARGET}" "../../features/defence.py" \
  "    if exit_code != 1:" "    if False:"
expect_reject "a patch path that escapes the copy by traversal" \
  "PATCH DID NOT APPLY" "${REPO}" --table "${WORK}/traversal.json"
AFTER="$(sha256sum features/defence.py | cut -d" " -f1)"
if [ "${BEFORE}" != "${AFTER}" ]; then
  echo "HARNESS EDITED THE REAL CHECKOUT: features/defence.py changed"
  FAILED=1
else
  echo "harness leaves the real checkout alone"
fi

set +e
ONLY_OUT="$(run_harness "${REPO}" --only " ")"
ONLY_RC=$?
set -e
if [ "${ONLY_RC}" -eq 0 ]; then
  echo "HARNESS DID NOT NOTICE: a selection that names no mutation"
  FAILED=1
elif ! printf '%s\n' "${ONLY_OUT}" | grep -q -- "--only names no mutation"; then
  echo "HARNESS FAILED FOR THE WRONG REASON: a selection that names no mutation"
  printf '%s\n' "${ONLY_OUT}" | tail -3
  FAILED=1
else
  echo "harness rejects: a selection that names no mutation"
fi

write_table "${WORK}/honest.json" "${HONEST_TARGET}" "features/defence.py" \
  "    if exit_code != 1:" "    if False:"
expect_accept "an honest mutation" "${REPO}" --table "${WORK}/honest.json"

if [ "${FAILED}" -ne 0 ]; then
  echo "HARNESS CHECK FAILED"
  exit 1
fi
echo "harness check passed"
