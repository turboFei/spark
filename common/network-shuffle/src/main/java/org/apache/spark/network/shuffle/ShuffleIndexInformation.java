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

import org.apache.spark.network.util.DigestUtils;

/**
 * Keeps the index information for a particular map output
 * as an in-memory LongBuffer.
 */
public class ShuffleIndexInformation {
  /** offsets as long buffer */
  private final LongBuffer offsets;
  private final boolean hasDigest;
  /** digests as long buffer */
  private final LongBuffer digests;
  private int size;

  public ShuffleIndexInformation(File indexFile) throws IOException {
    ByteBuffer offsetsBuffer, digestsBuffer;
    if (indexFile.length() % 8 == 0) {
      hasDigest = false;
      size = (int)indexFile.length();
      offsetsBuffer = ByteBuffer.allocate(size);
      offsets = offsetsBuffer.asLongBuffer();
      digestsBuffer = ByteBuffer.allocate(0);
      digests = digestsBuffer.asLongBuffer();
    } else {
      // this is a indexDigest file
      hasDigest = true;
      size = (int)indexFile.length() - 1;
      int digestLength = DigestUtils.getDigestLength();
      int blocks = (size - 8) / (8 + digestLength);
      offsetsBuffer = ByteBuffer.allocate((blocks + 1) * 8);
      offsets = offsetsBuffer.asLongBuffer();
      digestsBuffer = ByteBuffer.allocate(blocks * 8);
      digests = digestsBuffer.asLongBuffer();
    }
    try (DataInputStream dis = new DataInputStream(Files.newInputStream(indexFile.toPath()))) {
      if (hasDigest) {
        // The flag in head
        dis.readByte();
      }
      dis.readFully(offsetsBuffer.array());
      dis.readFully(digestsBuffer.array());
    }
  }

  /**
   * If this indexFile has digest
   */
  public boolean isHasDigest() {
    return hasDigest;
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
    /** Default digest is -1L.*/
    long digest = hasDigest ? digests.get(reduceId) : -1L;
    return new ShuffleIndexRecord(offset, nextOffset - offset, digest);
  }
}
