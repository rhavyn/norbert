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

/**
 * A <code>PartitionedLoadBalancer</code> handles calculating the next <code>Node</code> a message should be routed to
 * based on a PartitionedId.
 */
trait PartitionedLoadBalancer[PartitionedId] {
  /**
   * Returns the next <code>Node</code> a message should be routed to based on the PartitionId provided.
   *
   * @param id the id to be used to calculate partitioning information.
   *
   * @return the <code>Node</code> to route the next message to
   */
  def nextNode(id: PartitionedId): Option[Node]

//  def nodesForPartitions(partitions: Iterable[Int]): Set[Node]

  /**
   * Returns a list of nodes represents one replica of the cluster, this is used by the PartitionedNetworkClient to handle
   * broadcast to one replica
   *
   * @return the <code>Nodes</code> to broadcast the next message to a replica to
   */
  def nodesForOneReplica : Map[Node, Set[Int]]

}

/**
 * A factory which can generate <code>PartitionedLoadBalancer</code>s.
 */
trait PartitionedLoadBalancerFactory[PartitionedId] {
  /**
   * Create a new load balancer instance based on the currently available <code>Node</code>s.
   *
   * @param nodes the currently available <code>Node</code>s in the cluster
   *
   * @return a new <code>PartitionedLoadBalancer</code> instance
   * @throws InvalidClusterException thrown to indicate that the current cluster topology is invalid in some way and
   * it is impossible to create a <code>LoadBalancer</code>
   */
  @throws(classOf[InvalidClusterException])
  def newLoadBalancer(nodes: Set[Endpoint]): PartitionedLoadBalancer[PartitionedId]
}

/**
 * A component which provides a <code>PartitionedLoadBalancerFactory</code>.
 */
trait PartitionedLoadBalancerFactoryComponent[PartitionedId] {
  val loadBalancerFactory: PartitionedLoadBalancerFactory[PartitionedId]
}
