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

import cluster.{Node, InvalidClusterException}
import common.Endpoint

abstract class ConsistentHashPartitionedLoadBalancerFactory[PartitionedId](numPartitions: Int, serveRequestsIfPartitionMissing: Boolean = true) extends PartitionedLoadBalancerFactory[PartitionedId] {
  def newLoadBalancer(endpoints: Set[Endpoint]): PartitionedLoadBalancer[PartitionedId] = new PartitionedLoadBalancer[PartitionedId] with ConsistentHashLoadBalancerHelper {
    val partitionToNodeMap = generatePartitionToNodeMap(endpoints, numPartitions, serveRequestsIfPartitionMissing)

    def nextNode(id: PartitionedId) = nodeForPartition(partitionForId(id))

    // TODO: Speed up
    def nodesForOneReplica = endpoints.flatMap(_.node.partitionIds).foldLeft(Map.empty[Node, Set[Int]]) { case (map, partitionId) =>
      val nodeOption = nodeForPartition(partitionId)

      if(nodeOption.isDefined) {
        val node = nodeOption.get
        val partitions = map.getOrElse(node, Set.empty[Int]) + partitionId
        map + (node -> partitions)
      }
      else if(serveRequestsIfPartitionMissing) {
        log.warn("Partitions %s are unavailable, attempting to continue serving requests to other partitions.".format(partitionId))
        map
      }
      else
        throw new InvalidClusterException("Partitions %s are unavailable, cannot serve requests.".format(partitionId))
    }
  }

  /**
   * Calculates the id of the partition on which the specified <code>Id</code> resides.
   *
   * @param id the <code>Id</code> to map to a partition
   *
   * @return the id of the partition on which the <code>Id</code> resides
   */
  def partitionForId(id: PartitionedId): Int = {
    calculateHash(id).abs % numPartitions
  }

  /**
   * Hashes the <code>Id</code> provided. Users must implement this method. The <code>HashFunctions</code>
   * object provides an implementation of the FNV hash which may help in the implementation.
   *
   * @param id the <code>Id</code> to hash
   *
   * @return the hashed value
   */
  protected def calculateHash(id: PartitionedId): Int
}
