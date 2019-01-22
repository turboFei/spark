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
import org.apache.spark.network.buffer.NettyManagedBuffer;
import org.apache.spark.network.util.DigestUtils;

/**
 * Response to {@link ChunkFetchRequest} when a chunk exists and has been successfully fetched.
 *
 * Note that the server-side encoding of this messages does NOT include the buffer itself, as this
 * may be written by Netty in a more efficient manner (i.e., zero-copy write).
 * Similarly, the client-side decoding will reuse the Netty ByteBuf as the buffer.
 */
public final class ChunkFetchSuccess extends AbstractResponseMessage {
  public final StreamChunkId streamChunkId;
  public final byte[] digest;
  public final int digestLength;


  public ChunkFetchSuccess(StreamChunkId streamChunkId, ManagedBuffer buffer) {
    super(buffer, true);
    this.streamChunkId = streamChunkId;
    this.digest = new byte[0];
    this.digestLength = 0;
  }

  public ChunkFetchSuccess(StreamChunkId streamChunkId, ManagedBuffer buffer, byte[] digest) {
    super(buffer, true);
    this.streamChunkId = streamChunkId;
    this.digest = digest;
    this.digestLength = digest.length;
  }

  @Override
  public Type type() { return Type.ChunkFetchSuccess; }

  @Override
  public int encodedLength() {
    return streamChunkId.encodedLength() + 4 + digestLength;
  }

  /** Encoding does NOT include 'buffer' itself. See {@link MessageEncoder}. */
  @Override
  public void encode(ByteBuf buf) {
    streamChunkId.encode(buf);
    buf.writeInt(digestLength);
    buf.writeBytes(digest);
  }

  @Override
  public ResponseMessage createFailureResponse(String error) {
    return new ChunkFetchFailure(streamChunkId, error);
  }

  /** Decoding uses the given ByteBuf as our data, and will retain() it. */
  public static ChunkFetchSuccess decode(ByteBuf buf) {
    StreamChunkId streamChunkId = StreamChunkId.decode(buf);
    int digestLength = buf.readInt();
    byte[] digest = new byte[digestLength];
    buf.readBytes(digest);
    buf.retain();
    NettyManagedBuffer managedBuf = new NettyManagedBuffer(buf.duplicate());
    return new ChunkFetchSuccess(streamChunkId, managedBuf, digest);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(streamChunkId, body(), digestLength, digest);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof ChunkFetchSuccess) {
      ChunkFetchSuccess o = (ChunkFetchSuccess) other;
      return streamChunkId.equals(o.streamChunkId) && super.equals(o)
              && digestLength == o.digestLength && DigestUtils.digestEqual(digest, o.digest);
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamChunkId", streamChunkId)
      .add("digestLength", digestLength)
      .add("digest", DigestUtils.encodeHex(digest))
      .add("buffer", body())
      .toString();
  }
}
