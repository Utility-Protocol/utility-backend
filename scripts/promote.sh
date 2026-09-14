#!/usr/bin/env bash
set -e

STAGE=${1:-""}

if [ "$STAGE" = "staging" ]; then
  echo "Creating promotion PR: dev -> staging..."
  gh pr create --base staging --head dev --title "chore(release): promote dev to staging" --body "Automated promotion from dev to staging for pre-release testing."
elif [ "$STAGE" = "main" ]; then
  echo "Creating promotion PR: staging -> main..."
  gh pr create --base main --head staging --title "chore(release): promote staging to main" --body "Automated promotion from staging to production main branch."
else
  echo "Usage: ./scripts/promote.sh [staging|main]"
  echo "  staging: opens PR dev -> staging"
  echo "  main:    opens PR staging -> main"
  exit 1
fi
