#!/usr/bin/env bash
# Test fixture: blocks until stdin closes, then exits. Long-lived enough for
# Socket_dir_is_owner_only to inspect the socket dir while the process is alive.
read -r _ || true
