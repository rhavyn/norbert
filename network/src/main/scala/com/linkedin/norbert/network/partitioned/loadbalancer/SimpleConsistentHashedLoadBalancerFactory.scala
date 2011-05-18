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

import common.Endpoint
import java.util.TreeMap
import cluster.{Node, InvalidClusterException}

/**
 * This load balancer is appropriate when any server could handle the request. In this case, the partitions don't really mean anything. They simply control a percentage of the requests
 * that the node would receive. For instance, if node A had partitions 0,1,2 and node B had partitions 2,3, Node B would serve 40% of the traffic.
 */
class SimpleConsistentHashedLoadBalancerFactory[PartitionedId](numReplicas: Int, hashFn: PartitionedId => Int, endpointHashFn: String => Int) extends PartitionedLoadBalancerFactory[PartitionedId] {
  @throws(classOf[InvalidClusterException])
  def newLoadBalancer(endpoints: Set[Endpoint]): SimpleConsistentHashedLoadBalancer[PartitionedId] = {
    val wheel = new TreeMap[Int, Endpoint]

    endpoints.foreach { endpoint =>
      endpoint.node.partitionIds.foreach { partitionId =>
        (0 until numReplicas).foreach { r =>
          val node = endpoint.node
          var distKey = node.id + ":" + partitionId + ":" + node.url
          wheel.put(endpointHashFn(distKey), endpoint)
        }
      }
    }

    return new SimpleConsistentHashedLoadBalancer(wheel, hashFn)
  }
}

class SimpleConsistentHashedLoadBalancer[PartitionedId](wheel: TreeMap[Int, Endpoint], hashFn: PartitionedId => Int) extends PartitionedLoadBalancer[PartitionedId] {
  def nodesForOneReplica =
    throw new UnsupportedOperationException

  def nextNode(id: PartitionedId): Option[Node] = {
    PartitionUtil.searchWheel(wheel, hashFn(id), (e: Endpoint) => e.canServeRequests).map(_.node)
  }
}