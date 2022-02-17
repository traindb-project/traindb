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

export TRAINDB_ROOT_LOGGER_LEVEL=${traindb_root_logger_level:-info}
export TRAINDB_ROOT_LOGGER_APPENDER=${traindb_root_logger_appender:-rolling}

id=traindb-${TRAINDB_IDENT_STR}-server
log=$TRAINDB_LOG_DIR/${id}-${HOSTNAME}.log
pid=$TRAINDB_PID_DIR/${id}.pid

echo "Stopping TrainDB daemons"

if [ -f "$pid" ]; then
    target_pid=$(cat "$pid")
    if kill -0 $target_pid > /dev/null 2>&1; then
        echo "Stopping TrainDB server"
        kill $target_pid
        sleep ${TRAINDB_STOP_TIMEOUT:-5}
        if kill -0 $target_pid > /dev/null 2>&1; then
            echo "TrainDB server did not stop gracefully after $TRAINDB_STOP_TIMEOUT seconds: killing with kill -9"
            kill -9 $target_pid
        fi
    else
        echo "No TrainDB server to stop"
    fi
    rm -f "$pid"
else
    echo "No TrainDB server to stop"
fi
