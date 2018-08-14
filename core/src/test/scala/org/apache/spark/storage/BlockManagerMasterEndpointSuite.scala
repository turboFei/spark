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

package org.apache.spark.storage

import scala.concurrent.Future

import org.mockito.Mockito.{mock, when}
import org.scalatest.{BeforeAndAfterEach, Matchers, PrivateMethodTester}

import org.apache.spark._
import org.apache.spark.broadcast.BroadcastManager
import org.apache.spark.memory.UnifiedMemoryManager
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.network.BlockTransferService
import org.apache.spark.network.netty.NettyBlockTransferService
import org.apache.spark.rpc.RpcEnv
import org.apache.spark.scheduler.{LiveListenerBus, OutputCommitCoordinator}
import org.apache.spark.serializer.{KryoSerializer, SerializerManager}
import org.apache.spark.shuffle.sort.SortShuffleManager
import org.apache.spark.util.{ResetSystemProperties, SizeEstimator}

class BlockManagerMasterEndpointSuite extends SparkFunSuite with Matchers with BeforeAndAfterEach
  with PrivateMethodTester with LocalSparkContext with ResetSystemProperties {

  var context: SparkContext = null
  var bm: BlockManager = null
  var env: SparkEnv = null
  var conf: SparkConf = null
  var master: BlockManagerMaster = null
  var bcastManager: BroadcastManager = null
  // additional
  var rpcEnv: RpcEnv = null
  var securityMgr: SecurityManager = null
  var mapOutputTracker: MapOutputTrackerMaster = null
  var shuffleManager: SortShuffleManager = null
  // Reuse a serializer across tests to avoid creating a new thread-local buffer on each test
  var serializer: KryoSerializer = null
  var memManager: UnifiedMemoryManager = null
  var serializerManager: SerializerManager = null
  // me
  var bmme: BlockManagerMasterEndpoint = null

  private def makeBlockManager(
    maxMem: Long,
    name: String = SparkContext.DRIVER_IDENTIFIER,
    master: BlockManagerMaster = this.master,
    transferService: Option[BlockTransferService] = Option.empty): BlockManager = {
    conf.set("spark.testing.memory", maxMem.toString)
    conf.set("spark.memory.offHeap.size", maxMem.toString)

    val transfer = transferService
      .getOrElse(new NettyBlockTransferService(conf, securityMgr, "localhost", "localhost", 0, 1))

    val blockManager = new BlockManager(name, rpcEnv, master, serializerManager, conf,
      memManager, mapOutputTracker, shuffleManager, transfer, securityMgr, 0)
    memManager.setMemoryStore(blockManager.memoryStore)
    blockManager.initialize("app-id")
    blockManager
  }

  // Implicitly convert strings to BlockIds for test clarity.
  implicit def StringToBlockId(value: String): BlockId = new TestBlockId(value)
  def rdd(rddId: Int, splitId: Int): RDDBlockId = RDDBlockId(rddId, splitId)
  override def beforeEach(): Unit = {
    super.beforeEach()
    // Set the arch to 64-bit and compressedOops to true to get a deterministic test-case
    System.setProperty("os.arch", "amd64")
    conf = new SparkConf(false)
      .set("spark.app.id", "test")
      .set("spark.testing", "true")
      .set("spark.memory.fraction", "1")
      .set("spark.memory.storageFraction", "1")
      .set("spark.kryoserializer.buffer", "1m")
      .set("spark.test.useCompressedOops", "true")
      .set("spark.storage.unrollFraction", "0.4")
      .set("spark.storage.unrollMemoryThreshold", "512")
      .setAppName("testBlockManagerMasterEndpoint")
      .setMaster("local[*]")

    securityMgr = new SecurityManager(new SparkConf(false))
    mapOutputTracker = new MapOutputTrackerMaster(new SparkConf(false), bcastManager, true)
    shuffleManager = new SortShuffleManager(new SparkConf(false))
    // Reuse a serializer across tests to avoid creating a new thread-local buffer on each test
    serializer = new KryoSerializer(new SparkConf(false).set("spark.kryoserializer.buffer", "1m"))
    memManager = UnifiedMemoryManager(conf, numCores = 1)
    serializerManager = new SerializerManager(serializer, conf)
    // additional
    rpcEnv = RpcEnv.create("test", "localhost", 0, conf, securityMgr)
    conf.set("spark.driver.port", rpcEnv.address.port.toString)

    // Mock SparkContext to reduce the memory usage of tests. It's fine since the only reason we
    // need to create a SparkContext is to initialize LiveListenerBus.
    sc = mock(classOf[SparkContext])
    when(sc.conf).thenReturn(conf)
    bmme = new BlockManagerMasterEndpoint(rpcEnv, true, conf,
      new LiveListenerBus(sc))
    master = new BlockManagerMaster(rpcEnv.setupEndpoint("blockmanager",
      bmme), conf, true)
    bm = makeBlockManager(8000, "bm1", master)
    bcastManager = new BroadcastManager(true, conf, securityMgr)

    val initialize = PrivateMethod[Unit]('initialize)
    SizeEstimator invokePrivate initialize()
    env = new SparkEnv("executor-1", rpcEnv, serializer, serializer,
      serializerManager, mapOutputTracker, shuffleManager, bcastManager,
      bm, securityMgr, mock(classOf[MetricsSystem]),
      memManager, mock(classOf[OutputCommitCoordinator]), conf)
    SparkEnv.set(env)
  }

  override def afterEach(): Unit = {
    try {
      conf = null
      if(context != null) {
        context.stop()
        context = null
      }
      if(bcastManager != null) {
        bcastManager.stop()
        bcastManager = null
      }
      if(bm != null) {
        bm.stop()
        bm = null
      }
      if(master != null) {
        master.stop()
        master = null
      }
      if(bmme != null) {
        bmme.stop()
        bmme = null
      }
      if(master != null) {
        master.stop()
        master = null
      }
      if(master != null) {
        master.stop()
        master = null
      }
      if(rpcEnv != null) {
        rpcEnv.shutdown()
        rpcEnv = null
      }
      if(mapOutputTracker != null) {
//        mapOutputTracker.stop()
        mapOutputTracker = null
      }
      if(securityMgr != null) {
        securityMgr = null
      }
      if(shuffleManager != null) {
        shuffleManager.stop()
        shuffleManager = null
      }
      if(memManager != null) {
        memManager = null
      }
      if(serializerManager != null) {
        serializerManager = null
      }
      serializer = null
    } finally {
      super.afterEach()
    }
  }

  test( " test remove broadcast from broadcastManager") {
    val bcvar1 = bcastManager.newBroadcast(123, true)
    val bcid = bcvar1.id
    bcastManager.unbroadcast(bcid, true, true)

    var faile = false
    try {
      bcvar1.value
    } catch {
      case e: Exception =>
        faile = true
    }
    assert(faile == true, " remove success")

  }
  test( " test remove broadcast from blockmanager") {
    val bcvar1 = bcastManager.newBroadcast(123, true)
    val bcid = bcvar1.id
    bm.removeBroadcast(bcid, false)
    var faile = false
    try {
      bcvar1.value
    } catch {
      case e: Exception =>
        faile = true
    }
    assert(faile == true, " remove success")

  }
  test( " test remove broadcast from blockManagerMaster") {
    val bcvar1 = bcastManager.newBroadcast(123, true)
    val bcid = bcvar1.id
    master.removeBroadcast(bcid, true, true)
    var faile = false
    try {
      bcvar1.value
    } catch {
      case e: Exception =>
        faile = true
    }
    assert(faile == true, " remove success")

  }
  test( " test remove broadcast from blockManagerMasterEndPoint") {
    val bcvar1 = bcastManager.newBroadcast(123, true)
    val bcid = bcvar1.id
    bcastManager.unbroadcast(bcid, true, true)
    val removeBroadcast = PrivateMethod[Future[Seq[Int]]]('removeBroadcast)
    bmme invokePrivate removeBroadcast(bcid, true)
    var faile = false
    try {
      bcvar1.value
    } catch {
      case e: Exception =>
        faile = true
    }
    assert(faile == true, " remove success")
  }

}
