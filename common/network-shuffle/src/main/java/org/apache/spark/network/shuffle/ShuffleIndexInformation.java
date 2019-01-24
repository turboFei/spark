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

import io.netty.buffer.Unpooled;
import org.apache.spark.network.util.DigestUtils;

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
public class ShuffleIndexInformation {
  /** offsets as long buffer */
  private final LongBuffer offsets;
  private int size;
  private final ByteBuffer digests;
  private final int digestLength;

  public ShuffleIndexInformation(File indexFile, Boolean digestEnable, String digestAlgorithm) throws IOException {
    if (digestEnable) {
      size = (int) indexFile.length();
      digestLength = DigestUtils.getDigestLength(digestAlgorithm);
      int numPartitions = (size - 8) / (8 + digestLength);
      ByteBuffer buffer = ByteBuffer.allocate(numPartitions * 8 + 8);
      digests = ByteBuffer.allocate(numPartitions * digestLength);
      offsets = buffer.asLongBuffer();
      DataInputStream dis = null;
      try {
        dis = new DataInputStream(Files.newInputStream(indexFile.toPath()));
        dis.readFully(buffer.array());
        dis.readFully(digests.array());
      } finally {
        if (dis != null) {
          dis.close();
        }
      }
    } else {
      // for the compatible with previous version
      digestLength = 0;
      digests = null;
      size = (int)indexFile.length();
      ByteBuffer buffer = ByteBuffer.allocate(size);
      offsets = buffer.asLongBuffer();
      DataInputStream dis = null;
      try {
        dis = new DataInputStream(Files.newInputStream(indexFile.toPath()));
        dis.readFully(buffer.array());
      } finally {
        if (dis != null) {
          dis.close();
        }
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
  public ShuffleIndexRecord getIndex(int reduceId) {
    long offset = offsets.get(reduceId);
    long nextOffset = offsets.get(reduceId + 1);
    if (digestLength !=0 && digests != null) {
      byte[] digest = new byte[digestLength];
      digests.position(reduceId * digestLength);
      digests.get(digest);
      return new ShuffleIndexRecord(offset, nextOffset - offset, digest);
    } else {
      return new ShuffleIndexRecord(offset, nextOffset - offset);
    }
  }
}
