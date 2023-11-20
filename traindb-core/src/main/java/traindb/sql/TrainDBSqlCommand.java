/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package traindb.sql;

public abstract class TrainDBSqlCommand {
  public abstract Type getType();

  public enum Type {
    CREATE_MODELTYPE,
    DROP_MODELTYPE,
    SHOW_MODELTYPES,
    SHOW_MODELS,
    SHOW_TRAININGS,
    TRAIN_MODEL,
    DROP_MODEL,
    CREATE_SYNOPSIS,
    DROP_SYNOPSIS,
    SHOW_SYNOPSES,
    SHOW_SCHEMAS,
    SHOW_TABLES,
    SHOW_HYPERPARAMETERS,
    SHOW_PARTITIONS,
    USE_SCHEMA,
    DESCRIBE_TABLE,
    BYPASS_DDL_STMT,
    INCREMENTAL_QUERY,
    SHOW_QUERY_LOGS,
    SHOW_TASKS,
    DELETE_QUERY_LOGS,
    DELETE_TASKS,
    EXPORT_MODEL,
    IMPORT_MODEL,
    ALTER_MODEL_RENAME,
    ALTER_MODEL_ENABLE,
    ALTER_MODEL_DISABLE,
    EXPORT_SYNOPSIS,
    IMPORT_SYNOPSIS,
    ALTER_SYNOPSIS_RENAME,
    ALTER_SYNOPSIS_ENABLE,
    ALTER_SYNOPSIS_DISABLE,
    ANALYZE_SYNOPSIS,
    OTHER
  }
}