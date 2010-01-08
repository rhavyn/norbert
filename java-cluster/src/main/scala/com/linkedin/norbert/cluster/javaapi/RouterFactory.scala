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
import com.linkedin.norbert.cluster.router.{ConsistentHashRouterHelper, HashFunctions}

/**
 * A factory which can generate <code>Router</code>s.
 */
trait RouterFactory {
  /**
   * Create a new router instance based on the currently available <code>Node</code>s.
   *
   * @param nodes the currently available <code>Node</code>s in the cluster
   *
   * @return a new <code>Router</code> instance
   * @throws InvalidClusterException thrown to indicate that the current cluster topology is invalid in some way
   */
  @throws(classOf[InvalidClusterException])
  def newRouter(nodes: Array[Node]): Router
}

/**
 * A <code>Router</code> provides a mapping between an <code>Id</code> and a <code>Node</code> which
 * can process requests for the <code>Id</code>.
 */
trait Router {
  /**
   * Calculates a <code>Node</code> which can process a request for the specified id.
   *
   * @param id the id the request is being addressed to
   *
   * @return a <code>Node</code> which can process a request for the specified id
   */
  def calculateRoute(id: Int): Node
}

/**
 * A <code>RouterFactory</code> implementation that provides a consistent hash routing strategy.
 */
class ConsistentHashRouterFactory(numPartitions: Int) extends RouterFactory {
  def newRouter(nodes: Array[Node]) = new Router with ConsistentHashRouterHelper {
    protected val partitionToNodeMap = generatePartitionToNodeMap(nodes, numPartitions)

    def calculateRoute(id: Int) = nodeForPartition(getPartitionForId(id)).getOrElse(null)
  }

  private def getPartitionForId(id: Int) = HashFunctions.fnv(BigInt(id).toByteArray).abs % numPartitions
}
