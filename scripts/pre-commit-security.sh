#!/usr/bin/env bash
set -euo pipefail

# Fast, dependency-light guardrail for credentials that should never be committed.
# This complements code review and dedicated secret scanners in CI.
readonly EXCLUDED_PATHS='(^|/)(Cargo\.lock|target/|\.git/|\.pre-commit-cache/|node_modules/|\.env\.example$)'
readonly SECRET_PATTERN='(AKIA[0-9A-Z]{16}|-----BEGIN (RSA |DSA |EC |OPENSSH |PGP )?PRIVATE KEY-----|(?i)(api[_-]?key|secret|password|token)[[:space:]]*[:=][[:space:]]*[\"'"'"'][A-Za-z0-9_./+=:@%-]{20,}[\"'"'"'])'

if rg --hidden --line-number --glob '!target/**' --glob '!Cargo.lock' --glob '!*.md' \
  --regexp "${SECRET_PATTERN}" . | rg -v "${EXCLUDED_PATHS}"; then
  cat >&2 <<'MSG'
Potential secret material was found. Remove the credential, replace it with a documented
configuration variable, or add a narrowly scoped allowlist entry with security approval.
MSG
  exit 1
fi
