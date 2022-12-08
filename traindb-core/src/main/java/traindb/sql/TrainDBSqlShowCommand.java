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

abstract class TrainDBSqlShowCommand extends TrainDBSqlCommand {

  protected TrainDBSqlShowCommand() {
  }

  static class Modeltypes extends TrainDBSqlShowCommand {
    Modeltypes() {
      super();
    }

    @Override
    public Type getType() {
      return Type.SHOW_MODELTYPES;
    }
  }

  static class Models extends TrainDBSqlShowCommand {
    Models() {
      super();
    }

    @Override
    public Type getType() {
      return Type.SHOW_MODELS;
    }
  }

  static class Synopses extends TrainDBSqlShowCommand {
    Synopses() {
      super();
    }

    @Override
    public Type getType() {
      return Type.SHOW_SYNOPSES;
    }
  }

  static class Schemas extends TrainDBSqlShowCommand {
    Schemas() {
      super();
    }

    @Override
    public Type getType() {
      return Type.SHOW_SCHEMAS;
    }
  }

  static class Tables extends TrainDBSqlShowCommand {
    Tables() {
      super();
    }

    @Override
    public Type getType() {
      return Type.SHOW_TABLES;
    }
  }

  static class QueryLogs extends TrainDBSqlShowCommand {
    QueryLogs() {
      super();
    }

    @Override
    public Type getType() {
      return Type.SHOW_QUERY_LOGS;
    }
  }

  static class Tasks extends TrainDBSqlShowCommand {
    Tasks() {
      super();
    }

    @Override
    public Type getType() {
      return Type.SHOW_TASKS;
    }
  }
}
