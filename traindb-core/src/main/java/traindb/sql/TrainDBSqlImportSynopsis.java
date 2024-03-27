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

class TrainDBSqlImportSynopsis extends TrainDBSqlCommand {
  private final String synopsisName;
  private final String synopsisBinaryString;
  private final String importFilename;

  TrainDBSqlImportSynopsis(String synopsisName, String synopsisBinaryString,
                           String importFilename) {
    this.synopsisName = synopsisName;
    this.synopsisBinaryString = synopsisBinaryString;
    this.importFilename = importFilename;
  }

  String getSynopsisName() {
    return synopsisName;
  }

  String getSynopsisBinaryString() {
    return synopsisBinaryString;
  }

  String getImportFilename() {
    return importFilename;
  }

  @Override
  public Type getType() {
    return Type.IMPORT_SYNOPSIS;
  }
}
