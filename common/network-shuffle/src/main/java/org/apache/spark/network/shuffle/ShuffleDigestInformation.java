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
public class ShuffleDigestInformation {
  /** offsets as long buffer */
  private final ByteBuffer digestsBuffer;
  private final int digestLength;
  private int size;

  public ShuffleDigestInformation(File digestFile) throws IOException {
    size = (int)digestFile.length() - 3;
    digestsBuffer = ByteBuffer.allocate(size);
    DataInputStream dis = null;
    try {
      dis = new DataInputStream(Files.newInputStream(digestFile.toPath()));
      byte[] codec = new byte[3];
      dis.readFully(codec);
      digestLength = DigestUtils.getDigestLength(new String(codec));
      dis.readFully(digestsBuffer.array());
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
  public ShuffleDigestRecord getDigest(int reduceId) {
    byte[] digest = new byte[digestLength];
    digestsBuffer.position(reduceId * digestLength);
    digestsBuffer.get(digest);
    return new ShuffleDigestRecord(digest);
  }
}
