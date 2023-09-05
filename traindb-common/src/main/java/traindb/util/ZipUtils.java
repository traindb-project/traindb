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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
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
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;
import org.apache.commons.io.IOUtils;

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

  public static byte[] extractZipEntry(byte[] content, String filename) throws IOException {
    ZipInputStream zis = null;
    byte[] bytes = null;
    try {
      zis = new ZipInputStream(new ByteArrayInputStream(content));
      ZipEntry zipEntry;
      while ((zipEntry = zis.getNextEntry()) != null) {
        if (zipEntry.getName().equals(filename)) {
          bytes = zis.readAllBytes();
          break;
        }
      }
    } finally {
      if (zis != null) {
        zis.close();
      }
    }
    return bytes;
  }

  public static void unpack(byte[] content, String outputPath) throws IOException {
    ZipInputStream zis = null;

    try {
      File dir = new File(outputPath);
      if (!dir.exists()) {
        dir.mkdirs();
      }
      byte[] buffer = new byte[8192];
      zis = new ZipInputStream(new ByteArrayInputStream(content));
      ZipEntry zipEntry;
      while ((zipEntry = zis.getNextEntry()) != null) {
        String fileName = zipEntry.getName();
        File newFile = new File(outputPath + File.separator + fileName);
        new File(newFile.getParent()).mkdirs(); //create directories for sub directories in zip
        FileOutputStream fos = new FileOutputStream(newFile);
        int len;
        while ((len = zis.read(buffer)) > 0) {
          fos.write(buffer, 0, len);
        }
        fos.close();
        zis.closeEntry();
      }
    } finally {
      if (zis != null) {
        zis.close();
      }
    }
  }
}
