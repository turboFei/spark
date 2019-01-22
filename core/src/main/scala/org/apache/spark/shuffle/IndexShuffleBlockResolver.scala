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

package org.apache.spark.shuffle

import java.io._
import java.nio.channels.Channels
import java.nio.file.Files

import org.apache.spark.{SparkConf, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.io.NioBufferedFileInputStream
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.network.util.{DigestUtils, LimitedInputStream}
import org.apache.spark.shuffle.IndexShuffleBlockResolver.NOOP_REDUCE_ID
import org.apache.spark.storage._
import org.apache.spark.util.Utils

/**
 * Create and maintain the shuffle blocks' mapping between logic block and physical file location.
 * Data of shuffle blocks from the same map task are stored in a single consolidated data file.
 * The offsets of the data blocks in the data file are stored in a separate index file.
 *
 * We use the name of the shuffle data's shuffleBlockId with reduce ID set to 0 and add ".data"
 * as the filename postfix for data file, and ".index" as the filename postfix for index file.
 *
 */
// Note: Changes to the format in this file should be kept in sync with
// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getSortBasedShuffleBlockData().
private[spark] class IndexShuffleBlockResolver(
    conf: SparkConf,
    _blockManager: BlockManager = null)
  extends ShuffleBlockResolver
  with Logging {

  private lazy val blockManager = Option(_blockManager).getOrElse(SparkEnv.get.blockManager)

  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")

  // The digest conf for shuffle block check
  private final val algorithm = conf.get("spark.shuffle.digest.codec", "crc32")
  private val digestLength = DigestUtils.getDigestLength(algorithm)

  private lazy val nullDigestBytes = new Array[Byte](digestLength).map(_ => 1.toByte)

  def getDataFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleDataBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  private def getIndexFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, mapId, NOOP_REDUCE_ID))
  }

  /**
   * Remove data file and index file that contain the output data from one map.
   */
  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    var file = getDataFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting data ${file.getPath()}")
      }
    }

    file = getIndexFile(shuffleId, mapId)
    if (file.exists()) {
      if (!file.delete()) {
        logWarning(s"Error deleting index ${file.getPath()}")
      }
    }
  }

  /**
   * Check whether the given index and data files match each other.
   * If so, return the partition lengths in the data file. Otherwise return null.
   */
  private def checkIndexDigestAndDataFile(index: File, data: File, blocks: Int,
      digests: Array[Array[Byte]]): (Array[Long], Array[Array[Byte]]) = {
    // the index file should have `block + 1` longs as offset.
    if (index.length() != blocks * (8L + digestLength) + 8L) {
      return null
    }
    val lengths = new Array[Long](blocks)
    val digestArr = new Array[Array[Byte]](blocks)
    // Read the lengths of blocks
    val in = try {
      new DataInputStream(new NioBufferedFileInputStream(index))
    } catch {
      case e: IOException =>
        return null
    }
    try {
      // Convert the offsets into lengths of each block
      var offset = in.readLong()
      if (offset != 0L) {
        return null
      }
      var i = 0
      while (i < blocks) {
        val off = in.readLong()
        lengths(i) = off - offset
        offset = off
        i += 1
      }
      // read the digests
      val tempBytes = new Array[Byte](digestLength)
      i = 0
      while (i < blocks) {
        in.readFully(tempBytes)
        digestArr(i) = tempBytes.clone()
        i += 1
      }
    } catch {
      case e: IOException =>
        return null
    } finally {
      in.close()
    }

    // the size of data file should match with index file and the digests should match too
    if (data.length() == lengths.sum && !(0 until blocks).exists(i =>
      !DigestUtils.digestEqual(digestArr(i), digests(i)))) {
      (lengths, digestArr)
    } else {
      null
    }
  }

  /**
   * Write an index file with the offsets of each block, plus a final offset at the end for the
   * end of the output file. This will be used by getBlockData to figure out where each block
   * begins and ends.
   *
   * It will commit the data and index file as an atomic operation, use the existing ones, or
   * replace them with new ones.
   *
   * Note: the `lengths` will be updated to match the existing index file if use the existing ones.
   */
  def writeIndexFileAndCommit(
      shuffleId: Int,
      mapId: Int,
      lengths: Array[Long],
      dataTmp: File): Unit = {
    val indexFile = getIndexFile(shuffleId, mapId)
    val indexTmp = Utils.tempFileWith(indexFile)
    val digestArr = new Array[Array[Byte]](lengths.length)
    try {
      val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexTmp)))
      val dataIn = new FileInputStream(dataTmp);
      Utils.tryWithSafeFinally {
        // We take in lengths of each block, need to convert it to offsets.
        var offset = 0L
        out.writeLong(offset)
        for (length <- lengths) {
          offset += length
          out.writeLong(offset)
        }

        for (i <- (0 until lengths.length)) {
          val length = lengths(i)
          if (length == 0) {
            out.write(nullDigestBytes)
            digestArr(i) = nullDigestBytes.clone()
          } else {
            try {
              // here use md5
              val digest = DigestUtils.digestWithAlogrithm(algorithm,
                new LimitedInputStream(dataIn, length))
              out.write(digest)
              digestArr(i) = digest.clone()
            } catch {
              case e: IOException =>
                logError(s"NESPARK-160: Exception during make md5 for dataTmpFile $dataTmp ", e)
            }
          }
        }
      } {
        out.close()
      }

      val dataFile = getDataFile(shuffleId, mapId)
      // There is only one IndexShuffleBlockResolver per executor, this synchronization make sure
      // the following check and rename are atomic.
      synchronized {
        val existingLengthsDigest = checkIndexDigestAndDataFile(indexFile, dataFile,
          lengths.length, digestArr)
        if (existingLengthsDigest != null) {
          // Another attempt for the same task has already written our map outputs successfully,
          // so just use the existing partition lengths and delete our temporary map outputs.
          val existingLengths = existingLengthsDigest._1
          val existingDigests = existingLengthsDigest._2
          System.arraycopy(existingLengths, 0, lengths, 0, lengths.length)
          System.arraycopy(existingDigests, 0, digestArr, 0, digestArr.length)
          if (dataTmp != null && dataTmp.exists()) {
            dataTmp.delete()
          }
          indexTmp.delete()
        } else {
          // This is the first successful attempt in writing the map outputs for this task,
          // so override any existing index and data files with the ones we wrote.
          if (indexFile.exists()) {
            indexFile.delete()
          }
          if (dataFile.exists()) {
            dataFile.delete()
          }
          if (!indexTmp.renameTo(indexFile)) {
            throw new IOException("fail to rename file " + indexTmp + " to " + indexFile)
          }
          if (dataTmp != null && dataTmp.exists() && !dataTmp.renameTo(dataFile)) {
            throw new IOException("fail to rename file " + dataTmp + " to " + dataFile)
          }
        }
      }
    } finally {
      if (indexTmp.exists() && !indexTmp.delete()) {
        logError(s"Failed to delete temporary index file at ${indexTmp.getAbsolutePath}")
      }
    }
  }

  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    // The block is actually going to be a range of a single map output file for this map, so
    // find out the consolidated file, then the offset within that from our index
    val indexFile = getIndexFile(blockId.shuffleId, blockId.mapId)

    // SPARK-22982: if this FileInputStream's position is seeked forward by another piece of code
    // which is incorrectly using our file descriptor then this code will fetch the wrong offsets
    // (which may cause a reducer to be sent a different reducer's data). The explicit position
    // checks added here were a useful debugging aid during SPARK-22982 and may help prevent this
    // class of issue from re-occurring in the future which is why they are left here even though
    // SPARK-22982 is fixed.
    val channel = Files.newByteChannel(indexFile.toPath)
    channel.position(blockId.reduceId * 8L)
    val in = new DataInputStream(Channels.newInputStream(channel))
    try {
      val offset = in.readLong()
      val nextOffset = in.readLong()
      val actualPosition = channel.position()
      val expectedPosition = blockId.reduceId * 8L + 16
      if (actualPosition != expectedPosition) {
        throw new Exception(s"SPARK-22982: Incorrect channel position after index file reads: " +
          s"expected $expectedPosition but actual position was $actualPosition.")
      }
      val tempDigestBytes = new Array[Byte](digestLength)
      val reducerNum = (indexFile.length() - 8) / (8 + digestLength)
      channel.position((reducerNum + 1) * 8L + blockId.reduceId * digestLength)
      in.readFully(tempDigestBytes)
      val actualDigestPosition = channel.position()
      val exceptedDigestPosition = (reducerNum + 1) * 8L + (blockId.reduceId + 1) * digestLength
      if (actualDigestPosition != exceptedDigestPosition) {
        throw new Exception(s"NESPARK-160: Incorrect channel position after md5 file reads: " +
          s"expected $exceptedDigestPosition but actual position was $actualDigestPosition.")
      }
      new FileSegmentManagedBuffer(
        transportConf,
        getDataFile(blockId.shuffleId, blockId.mapId),
        offset,
        nextOffset - offset,
        tempDigestBytes)
    } finally {
      in.close()
    }
  }

  override def stop(): Unit = {}
}

private[spark] object IndexShuffleBlockResolver {
  // No-op reduce ID used in interactions with disk store.
  // The disk store currently expects puts to relate to a (map, reduce) pair, but in the sort
  // shuffle outputs for several reduces are glommed into a single file.
  val NOOP_REDUCE_ID = 0
}
