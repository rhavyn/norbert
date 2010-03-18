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

import com.linkedin.norbert.cluster.{InvalidClusterException, Node}

trait PartitionedLoadBalancerFactoryComponent {
  val loadBalancerFactory: PartitionedLoadBalancerFactory

  /**
   * The type of the id which the <code>LoadBalancer</code> can process.
   */
  type PartitionedId

  /**
   * A <code>LoadBalancer</code> handles calculating the next <code>Node</code> a message should be routed to.
   */
  trait PartitionedLoadBalancer {
    /**
     * Returns the next <code>Node</code> a message should be routed to based on the PartitionedId provided.
     *
     * @param id the id to be used to calculate partitioning information.  If partitioning is not desired None
     * will be passed in.
     *
     * @return the <code>Node</code> to route the next message to
     */
    def nextNode(id: PartitionedId): Option[Node]
  }

  /**
   * A factory which can generate <code>PartitionedLoadBalancer</code>s.
   */
  trait PartitionedLoadBalancerFactory {
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
    def newLoadBalancer(nodes: Seq[Node]): PartitionedLoadBalancer
  }
}
