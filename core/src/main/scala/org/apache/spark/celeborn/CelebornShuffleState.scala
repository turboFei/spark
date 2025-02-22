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

package org.apache.spark.celeborn

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import org.apache.spark.SparkEnv
import org.apache.spark.internal.config.ConfigBuilder

object CelebornShuffleState {

  private val CELEBORN_CLIENT_ADAPTIVE_OPTIMIZE_SKEWED_PARTITION_READ =
    ConfigBuilder("spark.celeborn.client.adaptive.optimizeSkewedPartitionRead.enabled")
      .booleanConf
      .createWithDefault(false)

  private val CELEBORN_STAGE_RERUN_ENABLED =
    ConfigBuilder("spark.celeborn.client.spark.stageRerun.enabled")
      .withAlternative("spark.celeborn.client.spark.fetch.throwsFetchFailure")
      .booleanConf
      .createWithDefault(false)

  private val celebornOptimizeSkewedPartitionReadEnabled = new AtomicBoolean()
  private val stageRerunEnabled = new AtomicBoolean()
  private val skewShuffleIds = ConcurrentHashMap.newKeySet[Int]()

  // call this from SparkEnv.create
  def init(env: SparkEnv): Unit = {
    // cleanup existing state (if required) - and initialize
    skewShuffleIds.clear()

    // use env.conf for all initialization, and not SQLConf
    celebornOptimizeSkewedPartitionReadEnabled.set(
      env.conf.get("spark.shuffle.manager", "sort").contains("celeborn") &&
      env.conf.get(CELEBORN_CLIENT_ADAPTIVE_OPTIMIZE_SKEWED_PARTITION_READ))
    stageRerunEnabled.set(env.conf.get(CELEBORN_STAGE_RERUN_ENABLED))
  }

  def unregisterCelebornSkewedShuffle(shuffleId: Int): Unit = {
    skewShuffleIds.remove(shuffleId)
  }

  def registerCelebornSkewedShuffle(shuffleId: Int): Unit = {
    skewShuffleIds.add(shuffleId)
  }

  def isCelebornSkewedShuffle(shuffleId: Int): Boolean = {
    skewShuffleIds.contains(shuffleId)
  }

  def celebornAdaptiveOptimizeSkewedPartitionReadEnabled: Boolean = {
    celebornOptimizeSkewedPartitionReadEnabled.get()
  }

  def celebornStageRerunEnabled: Boolean = {
    stageRerunEnabled.get()
  }

}
