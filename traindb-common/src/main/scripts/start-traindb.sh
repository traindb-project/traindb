#!/usr/bin/env bash

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

bin=$(dirname -- "${BASH_SOURCE-$0}")
bin=$(cd -- "$bin"; pwd)

. "$bin/traindb-config.sh"

echo "Starting TrainDB daemons"

id=traindb-${TRAINDB_IDENT_STR}-server
log=$TRAINDB_LOG_DIR/${id}-${HOSTNAME}.log
pid=$TRAINDB_PID_DIR/${id}.pid

[ -w "$TRAINDB_PID_DIR" ] || mkdir -p "$TRAINDB_PID_DIR"
if [ -f "$pid" ]; then
  target_pid=$(cat "$pid")
  if kill -0 $target_pid > /dev/null 2>&1; then
    echo "TrainDB server running as process ${target_pid}. Stop it first."
    exit 1
  fi
fi

echo "Starting TrainDB server, logging to $log"
cd "$TRAINDB_PREFIX"

TRAINDB_ROOT_LOGGER_LEVEL=${TRAINDB_ROOT_LOGGER_LEVEL:-INFO}
TRAINDB_ROOT_LOGGER_APPENDER=${TRAINDB_ROOT_LOGGER_APPENDER:-console}

TRAINDB_OPTS="$TRAINDB_OPTS -Dtraindb.rootLogger.level=$TRAINDB_ROOT_LOGGER_LEVEL"
TRAINDB_OPTS="$TRAINDB_OPTS -Dtraindb.rootLogger.appender=$TRAINDB_ROOT_LOGGER_APPENDER"

exec "$JAVA" -cp $CLASSPATH $TRAINDB_OPTS traindb.engine.TrainDBServer "$@" > "$log" 2>&1 < /dev/null &
echo $! > "$pid"
sleep 1
head "$log"

echo "ulimit -a for user $USER" >> "$log"
ulimit -a >> "$log" 2>&1
sleep 3

if ! ps -p $! > /dev/null; then
  exit 1
fi
