#!/usr/bin/env bash
# Test fixture: reports the NAMES (never the values) of its own environment on stderr, one per
# line, then exits, so a test can assert exactly which variables ConnectorProcess.Spawn forwarded.
env | cut -d= -f1 | sed "s/^/env-name: /" >&2
exit 0
