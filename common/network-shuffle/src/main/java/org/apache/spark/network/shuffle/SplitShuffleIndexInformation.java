/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.network.shuffle;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.LongBuffer;
import java.nio.file.Files;

/**
 * Keeps the index information for a particular map output
 * as an in-memory LongBuffer.
 */
public class SplitShuffleIndexInformation {
  /** offsets as long buffer */
  private  LongBuffer[] offsets;
  private int[] splitLengths;
  private int size;

  public SplitShuffleIndexInformation(File indexFile) throws IOException {
    size = (int)indexFile.length();
    DataInputStream dis = null;
    try {
      dis = new DataInputStream(Files.newInputStream(indexFile.toPath()));
      int reduceNum = (int)dis.readLong();
      offsets = new LongBuffer[reduceNum + 1];
      // the last store the total length
      splitLengths = new int[reduceNum + 1];
      long offset = dis.readLong();
      for (int i = 0; i < reduceNum; i ++) {
        long nextOffset = dis.readLong();
        splitLengths[i] = (int)(nextOffset - offset);
        offset = nextOffset;
      }
      // the last length is 1
      splitLengths[reduceNum] = 1;
      for (int i = 0; i <= reduceNum; i ++) {
        ByteBuffer buffer = ByteBuffer.allocate(splitLengths[i] * 8);
        dis.readFully(buffer.array());
        offsets[i] = buffer.asLongBuffer();
      }
    } finally {
      if (dis != null) {
        dis.close();
      }
    }
  }

  /**
   * Size of the index file
   * @return size
   */
  public int getSize() {
    return size;
  }

  /**
   * Get index offset for a particular reducer.
   */
  public ShuffleIndexRecord getIndex(int reduceId, int splitId) {
    long offset = offsets[reduceId].get(splitId);
    long nextOffset = splitLengths[reduceId] - 1 > splitId
            ? offsets[reduceId].get(splitId + 1) : offsets[reduceId + 1].get(0);
    return new ShuffleIndexRecord(offset, nextOffset - offset);
  }
}
