/*
 * Copyright 2009-2010 LinkedIn, Inc
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
package com.linkedin.norbert
package network
package partitioned
package loadbalancer

import cluster.{InvalidClusterException, Node}
import common.Endpoint
import java.util.concurrent.atomic.AtomicInteger
import annotation.tailrec

/**
 * A mixin trait that provides functionality to help implement a consistent hash based <code>Router</code>.
 */
trait ConsistentHashLoadBalancerHelper {

  /**
   * A mapping from partition id to the <code>Node</code>s which can service that partition.
   */
  protected val partitionToNodeMap: Map[Int, (IndexedSeq[Endpoint], AtomicInteger)]

  /**
   * Given the currently available <code>Node</code>s and the total number of partitions in the cluster, this method
   * generates a <code>Map</code> of partition id to the <code>Node</code>s which service that partition.
   *
   * @param nodes the current available nodes
   * @param numPartitions the total number of partitions in the cluster
   *
   * @return a <code>Map</code> of partition id to the <code>Node</code>s which service that partition
   * @throws InvalidClusterException thrown if every partition doesn't have at least one available <code>Node</code>
   * assigned to it
   */
  protected def generatePartitionToNodeMap(nodes: Set[Endpoint], numPartitions: Int): Map[Int, (IndexedSeq[Endpoint], AtomicInteger)] = {
    val partitionToNodeMap = (for (n <- nodes; p <- n.node.partitionIds) yield(p, n)).foldLeft(Map.empty[Int, IndexedSeq[Endpoint]]) {
      case (map, (partitionId, node)) => map + (partitionId -> (node +: map.get(partitionId).getOrElse(Vector.empty[Endpoint])))
    }

    for (i <- 0 until numPartitions) {
      if (!partitionToNodeMap.contains(i)) throw new InvalidClusterException("Partition %d is not assigned a node".format(i))
    }

    partitionToNodeMap.map { case (pId, endPoints) => pId -> (endPoints, new AtomicInteger(0)) }
  }

  /**
   * Calculates a <code>Node</code> which can service a request for the specified partition id.
   *
   * @param partitionId the id of the partition
   *
   * @return <code>Some</code> with the <code>Node</code> which can service the partition id, <code>None</code>
   * if there are no available <code>Node</code>s for the partition requested
   */
  protected def nodeForPartition(partitionId: Int): Option[Node] = nodeForPartition(partitionId, 0)

  @tailrec
  private def nodeForPartition(partitionId: Int, numIterations: Int): Option[Node] = {
    val endpoint = partitionToNodeMap.get(partitionId).map { case (endpoints, counter) =>
      endpoints(counter.getAndIncrement % endpoints.size)
    }

    val maxIterations = 4 * partitionToNodeMap.get(partitionId).map(_._1.size).getOrElse(0)

    endpoint match {
      case None => None
      case Some(endpoint) =>
        if(endpoint.canServeRequests)
          Some(endpoint.node)
        else if(numIterations > maxIterations)
          None
        else
          nodeForPartition(partitionId, numIterations + 1)
    }
  }
}
