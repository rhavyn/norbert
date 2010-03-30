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
package com.linkedin.norbert.network.partitioned.loadbalancer

import com.linkedin.norbert.cluster.Node

abstract class ConsistentHashPartitionedLoadBalancerFactory[PartitionedId](numPartitions: Int) extends PartitionedLoadBalancerFactory[PartitionedId] {
  def newLoadBalancer(nodes: Seq[Node]) = new PartitionedLoadBalancer[PartitionedId] with ConsistentHashLoadBalancerHelper {
    val partitionToNodeMap = generatePartitionToNodeMap(nodes, numPartitions)

    def nextNode(id: PartitionedId) = nodeForPartition(partitionForId(id))
  }

  /**
   * Calculates the id of the partition on which the specified <code>Id</code> resides.
   *
   * @param id the <code>Id</code> to map to a partition
   *
   * @return the id of the partition on which the <code>Id</code> resides
   */
  def partitionForId(id: PartitionedId): Int = calculateHash(id).abs % numPartitions

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
