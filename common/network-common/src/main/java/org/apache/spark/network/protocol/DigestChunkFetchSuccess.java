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
import io.netty.buffer.Unpooled;
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
public final class DigestChunkFetchSuccess extends AbstractResponseMessage {
  public final StreamChunkId streamChunkId;
  public final ByteBuf digestBuf;
  public final int digestLength;

  public DigestChunkFetchSuccess(StreamChunkId streamChunkId, ManagedBuffer buffer, ByteBuf digestBuf) {
    super(buffer, true);
    this.streamChunkId = streamChunkId;
    this.digestBuf = digestBuf;
    this.digestLength = digestBuf.readableBytes();
  }

  @Override
  public Type type() { return Type.DigestChunkFetchSuccess; }

  @Override
  public int encodedLength() {
    return streamChunkId.encodedLength() + 4 + digestLength;
  }

  /** Encoding does NOT include 'buffer' itself. See {@link MessageEncoder}. */
  @Override
  public void encode(ByteBuf buf) {
    streamChunkId.encode(buf);
    buf.writeInt(digestLength);
    buf.writeBytes(digestBuf.array());
  }

  @Override
  public ResponseMessage createFailureResponse(String error) {
    return new ChunkFetchFailure(streamChunkId, error);
  }

  /** Decoding uses the given ByteBuf as our data, and will retain() it. */
  public static DigestChunkFetchSuccess decode(ByteBuf buf) {
    StreamChunkId streamChunkId = StreamChunkId.decode(buf);
    int digestLength = buf.readInt();
    byte[] digest = new byte[digestLength];
    buf.readBytes(digest);
    ByteBuf digestBuf = Unpooled.wrappedBuffer(digest);
    buf.retain();
    // retain the digestBuf
    digestBuf.retain();
    NettyManagedBuffer managedBuf = new NettyManagedBuffer(buf.duplicate());
    return new DigestChunkFetchSuccess(streamChunkId, managedBuf, digestBuf);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(streamChunkId, body(), digestLength, digestBuf);
  }

  @Override
  public boolean equals(Object other) {
    if (other instanceof DigestChunkFetchSuccess) {
      DigestChunkFetchSuccess o = (DigestChunkFetchSuccess) other;
      return streamChunkId.equals(o.streamChunkId) && super.equals(o)
              && digestLength == o.digestLength && DigestUtils.digestEqual(digestBuf.array(), o.digestBuf.array());
    }
    return false;
  }

  @Override
  public String toString() {
    return Objects.toStringHelper(this)
      .add("streamChunkId", streamChunkId)
      .add("digestLength", digestLength)
      .add("digestHex", DigestUtils.encodeHex(digestBuf.array()))
      .add("buffer", body())
      .toString();
  }
}
