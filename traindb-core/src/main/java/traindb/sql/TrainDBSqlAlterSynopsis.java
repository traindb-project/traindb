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

abstract class TrainDBSqlAlterSynopsis extends TrainDBSqlCommand {

  private final String synopsisName;
  private final String newSynopsisName;

  protected TrainDBSqlAlterSynopsis(String synopsisName, String newSynopsisName) {
    this.synopsisName = synopsisName;
    this.newSynopsisName = newSynopsisName;
  }

  public String getSynopsisName() {
    return synopsisName;
  }

  public String getNewSynopsisName() {
    return newSynopsisName;
  }

  static class Rename extends TrainDBSqlAlterSynopsis {
    Rename(String synopsisName, String newSynopsisName) {
      super(synopsisName, newSynopsisName);
    }

    @Override
    public Type getType() {
      return Type.ALTER_SYNOPSIS_RENAME;
    }
  }

  static class Enable extends TrainDBSqlAlterSynopsis {
    Enable(String synopsisName) {
      super(synopsisName, null);
    }

    @Override
    public Type getType() {
      return Type.ALTER_SYNOPSIS_ENABLE;
    }
  }

  static class Disable extends TrainDBSqlAlterSynopsis {
    Disable(String synopsisName) {
      super(synopsisName, null);
    }

    @Override
    public Type getType() {
      return Type.ALTER_SYNOPSIS_DISABLE;
    }
  }
}
