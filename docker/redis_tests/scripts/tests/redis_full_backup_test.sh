#!/bin/sh
set -e -x

export WALG_FILE_PREFIX='/tmp/wal-g-test-data'
export WALG_STREAM_CREATE_COMMAND="redis-cli -a {password} --rdb /dev/stdout"
export WALG_STREAM_RESTORE_COMMAND="redis-server --dbfilename stdin --dir /dev"

sleep 10

service redis-server start

sleep 10

redis-cli set key test_value

expected_output=$(redis-cli get key)

mkdir $WALG_FILE_PREFIX
wal-g backup-push

redis-cli FLUSHALL
redis-cli shutdown

sleep 10

wal-g backup-list

sleep 10

wal-g backup-fetch LATEST

sleep 10

actual_output=$(redis-cli get key)

if [ "$actual_output" != "$expected_output" ]; then
  echo "Error: actual output doesn't match expected output"
  echo "Expected output: $expected_output"
  echo "Actual output: $actual_output"
  exit 1
fi

redis-cli shutdown
