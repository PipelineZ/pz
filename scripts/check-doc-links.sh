#!/usr/bin/env bash
# Checks that every relative markdown link in the repository's public markdown -- the root
# README, docs/, connectors/, samples/, scripts/, and .github/ -- resolves to an existing file.
# Anchors (#...) are stripped before the existence check; external links (http/https/mailto)
# are ignored. Exits 1 and lists offenders if any link is broken. Build outputs (bin/, obj/)
# and .git/ are excluded as generated or non-published.
set -euo pipefail
cd "$(dirname "$0")/.."

fail=0
while IFS= read -r file; do
  # extract targets of [text](target) links
  while IFS= read -r link; do
    target="${link%%#*}"                       # strip anchor
    [[ -z "$target" ]] && continue             # pure-anchor link (#section) — same-file, ok
    [[ "$target" =~ ^(https?|mailto): ]] && continue
    resolved="$(dirname "$file")/$target"
    if [[ ! -e "$resolved" ]]; then
      echo "BROKEN  $file: $link"
      fail=1
    fi
  done < <(grep -oE '\]\(([^)]+)\)' "$file" | sed -E 's/^\]\(//; s/\)$//')
done < <(find . -name '*.md' \
  -not -path './.git/*' \
  -not -path '*/bin/*' \
  -not -path '*/obj/*' \
  -not -path './.claude/*' | sed 's|^\./||')

exit "$fail"
