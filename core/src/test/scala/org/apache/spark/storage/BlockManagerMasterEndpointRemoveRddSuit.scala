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

import org.scalatest.{BeforeAndAfterEach, ConfigMap}

import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}

class BlockManagerMasterEndpointRemoveRddSuit extends
  SparkFunSuite with BeforeAndAfterEach {

  private var conf: SparkConf = null
  private var context: SparkContext = null

  override def beforeEach(): Unit = {
    super.beforeEach()
    conf = new SparkConf().setAppName("testRemoveRdd")
        .setMaster("local")
    context = new SparkContext(conf)
  }

  override def afterEach(configMap: ConfigMap): Unit = {
    super.afterAll()
    conf = null
    if(context != null) {
      context.stop()
      context = null
    }

  }

  test("remove rdd from Memory") {
    val rdd1 = context.parallelize( 1 to 10, 2)
    val rdd2 = rdd1.map((_, 1))
    rdd2.persist(StorageLevel.MEMORY_ONLY)
    val rdd3 = rdd2.reduceByKey(_ + _)
    rdd3.collect()
    rdd2.unpersist()
    assert(rdd2.getStorageLevel==StorageLevel.NONE, "failed to unpersist rdd from memory")
  }
  test("remove rdd from Disk") {
    val rdd1 = context.parallelize( 1 to 10, 2)
    val rdd2 = rdd1.map((_, 1))
    rdd2.persist(StorageLevel.DISK_ONLY)
    val rdd3 = rdd2.reduceByKey(_ + _)
    rdd3.collect()
    rdd2.unpersist()
    assert(rdd2.getStorageLevel==StorageLevel.NONE, "failed to unpersist rdd  from disk")
  }
  test("remove rdd from Memory_AND_DISK") {
    val rdd1 = context.parallelize( 1 to 10, 2)
    val rdd2 = rdd1.map((_, 1))
    rdd2.persist(StorageLevel.MEMORY_AND_DISK)
    val rdd3 = rdd2.reduceByKey(_ + _)
    rdd3.collect()
    rdd2.unpersist()
    assert(rdd2.getStorageLevel==StorageLevel.NONE, "failed to unpersist rdd from memory_and_disk")
  }
  test("remove rdd from Memory_Ser") {
    val rdd1 = context.parallelize( 1 to 10, 2)
    val rdd2 = rdd1.map((_, 1))
    rdd2.persist(StorageLevel.MEMORY_ONLY_SER)
    val rdd3 = rdd2.reduceByKey(_ + _)
    rdd3.collect()
    rdd2.unpersist()
    assert(rdd2.getStorageLevel==StorageLevel.NONE, "failed to unpersist rdd from memory_ser ")
  }
  test("remove rdd from MEMORY_AND_DISK_SER") {
    val rdd1 = context.parallelize( 1 to 10, 2)
    val rdd2 = rdd1.map((_, 1))
    rdd2.persist(StorageLevel.MEMORY_AND_DISK_SER)
    val rdd3 = rdd2.reduceByKey(_ + _)
    rdd3.collect()
    rdd2.unpersist()
    assert(rdd2.getStorageLevel==StorageLevel.NONE,
      "failed to unpersist rdd  from memory_and_disk_ser")
  }



}
