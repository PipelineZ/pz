#!/usr/bin/env bash
# Test fixture: spawns a child that outlives a naive single-pid kill, so
# Dispose_kills_the_process_group can assert the whole group dies together.
sleep 300 &
wait
