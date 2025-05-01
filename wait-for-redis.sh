#!/bin/bash
# wait-for-redis.sh

set -e

host="$1"
port="$2"
shift 2
cmd="$@"

until redis-cli -h "$host" -p "$port" ping | grep -q 'PONG'; do
  >&2 echo "Redis está indisponível"
  sleep 1
done

>&2 echo "Redis está pronto. Executando comando..."
exec $cmd