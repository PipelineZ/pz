#!/usr/bin/env bash
# Test fixture: writes a known line to stderr then exits nonzero, so
# Stderr_is_captured_as_ring_buffer can assert the line landed in StderrTail.
echo "die.sh: known failure line" >&2
exit 1
