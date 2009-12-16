/*
 * Copyright 2009 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.norbert.cluster.javaapi

import com.linkedin.norbert.cluster.{InvalidClusterException, Node}
import com.linkedin.norbert.cluster.router.{ConsistentHashRouterHelper, ConsistentHashRouterFactoryHelper, HashFunctions}

trait RouterFactory {
  @throws(classOf[InvalidClusterException])
  def newRouter(nodes: Array[Node]): Router
}

trait Router {
  def calculateRoute(id: Int): Node
}

class ConsistentHashRouterFactory(numPartitions: Int) extends RouterFactory with ConsistentHashRouterFactoryHelper {
  def newRouter(nodes: Array[Node]) = new Router with ConsistentHashRouterHelper {
    protected val partitionToNodeMap = generatePartitionToNodeMap(nodes, numPartitions)

    def calculateRoute(id: Int) = nodeForPartition(getPartitionForId(id)).getOrElse(null)
  }

  private def getPartitionForId(id: Int) = HashFunctions.fnv(BigInt(id).toByteArray).abs % numPartitions
}
