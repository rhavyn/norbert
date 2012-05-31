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
import client.loadbalancer.LoadBalancerHelpers
import logging.Logging

/**
 * A mixin trait that provides functionality to help implement a hash based <code>Router</code>.
 */
trait DefaultLoadBalancerHelper extends LoadBalancerHelpers with Logging {
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
  protected def generatePartitionToNodeMap(nodes: Set[Endpoint], numPartitions: Int, serveRequestsIfPartitionMissing: Boolean): Map[Int, (IndexedSeq[Endpoint], AtomicInteger)] = {
    val partitionToNodeMap = (for (n <- nodes; p <- n.node.partitionIds) yield(p, n)).foldLeft(Map.empty[Int, IndexedSeq[Endpoint]]) {
      case (map, (partitionId, node)) => map + (partitionId -> (node +: map.get(partitionId).getOrElse(Vector.empty[Endpoint])))
    }

    val possiblePartitions = (0 until numPartitions).toSet
    val missingPartitions = possiblePartitions diff (partitionToNodeMap.keys.toSet)

    if(missingPartitions.size == possiblePartitions.size)
      throw new InvalidClusterException("Every single partition appears to be missing. There are %d partitions".format(numPartitions))
    else if(!missingPartitions.isEmpty) {
      if(serveRequestsIfPartitionMissing)
        log.warn("Partitions %s are unavailable, attempting to continue serving requests to other partitions.".format(missingPartitions))
      else
        throw new InvalidClusterException("Partitions %s are unavailable, cannot serve requests.".format(missingPartitions))
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
  protected def nodeForPartition(partitionId: Int): Option[Node] = {
    partitionToNodeMap.get(partitionId) match {
      case None =>
        return None
      case Some((endpoints, counter)) =>
        val idx = counter.getAndIncrement
        var i = idx
        do {
          val endpoint = endpoints(i % endpoints.size)
          if(endpoint.canServeRequests)
            return Some(endpoint.node)

          i = i + 1
        } while(i != idx)

        return Some(endpoints(idx).node)
    }
  }



}
