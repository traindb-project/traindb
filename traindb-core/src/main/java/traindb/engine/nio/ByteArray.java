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

package traindb.engine.nio;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public final class ByteArray {

  private byte[] array;
  private int offset;
  private int length;

  /**
   * Create an instance of this class that wraps ths given array.
   * This class does not make a copy of the array, it just saves the reference.
   */
  public ByteArray(byte[] array, int offset, int length) {
    this.array = array;
    this.offset = offset;
    this.length = length;
  }

  public ByteArray(byte[] array) {
    this(array, 0, array.length);
  }

  public ByteArray() {
    this(null, 0, 0);
  }

  public void setBytes(byte[] array) {
    this.array = array;
    offset = 0;
    length = array.length;
  }

  public void setBytes(byte[] array, int length) {
    this.array = array;
    this.offset = 0;
    this.length = length;
  }

  public void setBytes(byte[] array, int offset, int length) {
    this.array = array;
    this.offset = offset;
    this.length = length;
  }


  public boolean equals(Object other) {
    if (other instanceof ByteArray) {
      ByteArray ob = (ByteArray) other;
      return ByteArray.equals(array, offset, length, ob.array, ob.offset, ob.length);
    }
    return false;
  }

  public int hashCode() {

    byte[] larray = array;

    int hash = length;
    for (int i = 0; i < length; i++) {
      hash += larray[i + offset];
    }
    return hash;
  }

  public final byte[] getArray() {
    return array;
  }

  public final int getOffset() {
    return offset;
  }

  public final int getLength() {
    return length;
  }

  public final void setLength(int newLength) {
    length = newLength;
  }

  public void readExternal(ObjectInput in) throws IOException {
    int len = length = in.readInt();
    offset = 0;
    array = new byte[len];

    in.readFully(array, 0, len);
  }

  public void writeExternal(ObjectOutput out) throws IOException {
    out.writeInt(length);
    out.write(array, offset, length);
  }

  private static boolean equals(byte[] a, int aOffset, int aLength, byte[] b, int bOffset,
                                int bLength) {
    if (aLength != bLength) {
      return false;
    }
    for (int i = 0; i < aLength; i++) {
      if (a[i + aOffset] != b[i + bOffset]) {
        return false;
      }
    }
    return true;
  }
}