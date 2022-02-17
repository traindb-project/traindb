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

# included in all the traindb scripts with source command
# should not be executed directly
# also should not be passed any arguments, since we need original $*

bin=$(dirname -- "${BASH_SOURCE-$0}")
bin=$(cd -- "$bin"; pwd -P)

TRAINDB_DEFAULT_PREFIX=$(cd -- "$bin/.."; pwd -P)
TRAINDB_PREFIX=${TRAINDB_PREFIX:-$TRAINDB_DEFAULT_PREFIX}
export TRAINDB_PREFIX

if [ $# -gt 1 ]; then
    if [ "--config" = "$1" ]; then
        shift
        confdir=$1
        if [ ! -d "$confdir" ]; then
            echo "Error: Cannot find configuration directory: $confdir" >&2
            exit 1
        fi
        shift
        TRAINDB_CONF_DIR=$confdir
    fi
fi

TRAINDB_CONF_DIR=${TRAINDB_CONF_DIR:-$TRAINDB_PREFIX/conf}
export TRAINDB_CONF_DIR

if [ -f "$TRAINDB_CONF_DIR/traindb-env.sh" ]; then
    . "$TRAINDB_CONF_DIR/traindb-env.sh"
fi

if [ -z "$JAVA_HOME" ]; then
    echo "Error: JAVA_HOME is not set" >&2
    exit 1
fi

JAVA=$JAVA_HOME/bin/java
JAVA_HEAP_MAX=-Xmx16G

if [ -n "$TRAINDB_HEAPSIZE" ]; then
    JAVA_HEAP_MAX=-Xmx${TRAINDB_HEAPSIZE}m
fi

TRAINDB_OPTS="$JAVA_HEAP_MAX"

# uncomment if you want to attach a debugger
#TRAINDB_OPTS="$TRAINDB_OPTS -agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"

lines=$("$JAVA" -version 2>&1 | tr '\r' '\n')
ver=$(echo $lines | sed -e 's/.*version "\(.*\)"\(.*\)/\1/; 1q')
if [[ $ver = "1."* ]]; then
    JAVA_VERSION=$(echo $ver | sed -e 's/1\.\([0-9]*\)\(.*\)/\1/; 1q')
else
    JAVA_VERSION=$(echo $ver | sed -e 's/\([0-9]*\)\(.*\)/\1/; 1q')
fi

if [[ $JAVA_VERSION -gt 8 ]]; then
    TRAINDB_OPTS="$TRAINDB_OPTS --add-opens java.base/java.util=ALL-UNNAMED"
fi

# classpath
TRAINDB_BASE_CLASSPATH=$TRAINDB_CONF_DIR

TRAINDB_JARS_DIR=${TRAINDB_JARS_DIR:-share/traindb}
TRAINDB_BASE_CLASSPATH=$TRAINDB_BASE_CLASSPATH:$TRAINDB_PREFIX/$TRAINDB_JARS_DIR/*

TRAINDB_LIB_JARS_DIR=${TRAINDB_LIB_JARS_DIR:-share/traindb/lib}
TRAINDB_BASE_CLASSPATH=$TRAINDB_BASE_CLASSPATH:$TRAINDB_PREFIX/$TRAINDB_LIB_JARS_DIR/*

TRAINDB_EXT_JARS_DIR=${TRAINDB_EXT_JARS_DIR:-share/traindb/ext}
TRAINDB_BASE_CLASSPATH=$TRAINDB_BASE_CLASSPATH:$TRAINDB_PREFIX/$TRAINDB_EXT_JARS_DIR/*

CLASSPATH=$TRAINDB_BASE_CLASSPATH:$CLASSPATH

TRAINDB_IDENT_STR=${TRAINDB_IDENT_STR:-$USER}
TRAINDB_PID_DIR=${TRAINDB_PID_DIR:-/tmp}

# logging
TRAINDB_LOG_DIR=${TRAINDB_LOG_DIR:-$TRAINDB_PREFIX/logs}
if [ ! -w "$TRAINDB_LOG_DIR" ]; then
    mkdir -p "$TRAINDB_LOG_DIR"
    chown "$USER" "$TRAINDB_LOG_DIR"
fi
export TRAINDB_LOG_DIR

TRAINDB_LOG_FILE=${TRAINDB_LOG_FILE:-traindb.log}
export TRAINDB_LOG_FILE
TRAINDB_LOG_LEVEL=${TRAINDB_LOG_LEVEL:-INFO}
export TRAINDB_LOG_LEVEL

TRAINDB_OPTS="$TRAINDB_OPTS -Dtraindb.log.dir=$TRAINDB_LOG_DIR"
TRAINDB_OPTS="$TRAINDB_OPTS -Dtraindb.log.file=$TRAINDB_LOG_FILE"
TRAINDB_OPTS="$TRAINDB_OPTS -Dtraindb.log.level=$TRAINDB_LOG_LEVEL"
