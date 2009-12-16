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
package com.linkedin.norbert.cluster.router

import com.linkedin.norbert.cluster.{InvalidClusterException, Node}

trait ConsistentHashRouterFactoryHelper {
  protected def generatePartitionToNodeMap(nodes: Seq[Node], numPartitions: Int): Map[Int, Seq[Node]] = {
    val partitionToNodeMap = (for (n <- nodes; p <- n.partitions) yield (p, n)).foldLeft(Map.empty[Int, List[Node]].withDefaultValue(Nil)) {
      case (map, (partitionId, node)) => map(partitionId) = node :: map(partitionId)
    }

    for (i <- 0 until numPartitions)
      if (!partitionToNodeMap.isDefinedAt(i)) throw new InvalidClusterException("Partition %d is not assigned a node".format(i))

    partitionToNodeMap
  }
}

trait ConsistentHashRouterHelper {
  private val random = new scala.util.Random()
  protected val partitionToNodeMap: Map[Int, Seq[Node]]  

  protected def nodeForPartition(partition: Int): Option[Node] = partitionToNodeMap.get(partition).map { nodes =>
    val i = random.nextInt % nodes.size
    nodes(i)
  }
}
