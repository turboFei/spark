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

package org.apache.spark.network.protocol;

import com.google.common.base.Objects;
import io.netty.buffer.ByteBuf;

import org.apache.spark.network.buffer.ManagedBuffer;

/**
 * Response to {@link StreamRequest} when the stream has been successfully opened.
 * <p>
 * Note the message itself does not contain the stream data. That is written separately by the
 * sender. The receiver is expected to set a temporary channel handler that will consume the
 * number of bytes this message says the stream has.
 */
public final class StreamResponse extends AbstractResponseMessage {
  public final String streamId;
  public final long byteCount;
  public final String md5Hex;
  public final byte md5Flag;

  public StreamResponse(String streamId, long byteCount, ManagedBuffer buffer) {
    super(buffer, false);
    this.streamId = streamId;
    this.byteCount = byteCount;
    this.md5Flag = 0;
    this.md5Hex = "";
  }

  public StreamResponse(String streamId, long byteCount, ManagedBuffer buffer, String md5Hex) {
    super(buffer, false);
    this.streamId = streamId;
    this.byteCount = byteCount;
    this.md5Flag = 1;
    this.md5Hex = md5Hex;
  }

  @Override
  public Type type() { return Type.StreamResponse; }

  @Override
  public int encodedLength() {
    return 8 + Encoders.Strings.encodedLength(streamId) + 1 + md5Hex.length();
  }

  /** Encoding does NOT include 'buffer' itself. See {@link MessageEncoder}. */
  @Override
  public void encode(ByteBuf buf) {
    Encoders.Strings.encode(buf, streamId);
    buf.writeLong(byteCount);
    buf.writeByte(md5Flag);
    buf.writeBytes(md5Hex.getBytes());
  }

  @Override
  public ResponseMessage createFailureResponse(String error) {
    return new StreamFailure(streamId, error);
  }

  public static StreamResponse decode(ByteBuf buf) {
    String streamId = Encoders.Strings.decode(buf);
    long byteCount = buf.readLong();
    byte md5Flag = buf.readByte();
    String md5Hex = "";
    if (md5Flag == 1) {
      byte[] readBytes = new byte[32];
      buf.readBytes(readBytes);
      md5Hex = new String(readBytes);
    }
    return new StreamResponse(streamId, byteCount, null, md5Hex);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(byteCount, streamId, body(), md5Hex);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof StreamResponse) {
      StreamResponse o = (StreamResponse) other;
      return byteCount == o.byteCount && streamId.equals(o.streamId)
              && md5Hex.equals(o.md5Hex);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamId", streamId)
      .add("byteCount", byteCount)
      .add("md5Flag", md5Flag)
      .add("md5Hex", md5Hex)
      .add("body", body())
      .toString();
  }

}
