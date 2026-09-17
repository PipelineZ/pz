#!/usr/bin/env bash
# Test fixture: writes far more to stdout than an OS pipe buffers (~64 KB on linux), some of it with
# no newline at all, then reports on stderr and exits 0. A host that redirects stdout without reading
# it leaves this script blocked in write() forever, so Chatty_stdout_does_not_block_the_child can
# tell a drained pipe from an undrained one by whether the script ever finishes.
head -c 1048576 /dev/zero | tr '\0' 'x'
for i in $(seq 1 2000); do
  echo "chatty.sh: progress line $i with some padding to make it longer than a few bytes"
done
echo "chatty.sh: finished writing" >&2
exit 0
