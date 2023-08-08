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

package traindb.util;

import java.io.IOException;
import java.io.Writer;
import java.net.URI;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public final class ZipUtils {

  private ZipUtils() {
  }

  public static void pack(String sourceDirPath, String zipFilePath) throws IOException {
    Path zp = Files.createFile(Paths.get(zipFilePath));
    try (ZipOutputStream zs = new ZipOutputStream(Files.newOutputStream(zp))) {
      Path sp = Paths.get(sourceDirPath);
      Files.walk(sp)
          .filter(path -> !Files.isDirectory(path))
          .forEach(path -> {
            ZipEntry zipEntry = new ZipEntry(sp.relativize(path).toString());
            try {
              zs.putNextEntry(zipEntry);
              Files.copy(path, zs);
              zs.closeEntry();
            } catch (IOException e) {
              throw new RuntimeException(e);
            }
          });
    }
  }

  public static void addFileToZip(Path file, Path zip) throws IOException {
    Map<String, String> env = new HashMap<>();
    env.put("create", "false");

    URI uri = URI.create("jar:file:" + zip.toString());
    try (FileSystem fs = FileSystems.newFileSystem(uri, env)) {
      Path p = fs.getPath(file.getFileName().toString());
      Files.copy(file, p, StandardCopyOption.REPLACE_EXISTING);
    }
  }

  public static void addNewFileFromStringToZip(String newFilename, String contents, Path zip)
      throws IOException {
    Map<String, String> env = new HashMap<>();
    env.put("create", "false");

    URI uri = URI.create("jar:file:" + zip.toString());
    try (FileSystem fs = FileSystems.newFileSystem(uri, env)) {
      Path p = fs.getPath(newFilename);
      try (Writer writer = Files.newBufferedWriter(p, StandardOpenOption.CREATE)) {
        writer.write(contents);
      }
    }
  }
}
