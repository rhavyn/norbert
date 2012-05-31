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
package client
package loadbalancer

import cluster.{InvalidClusterException, Node}
import common.Endpoint

/**
 * A <code>LoadBalancer</code> handles calculating the next <code>Node</code> a message should be routed to.
 */
trait LoadBalancer {
  /**
   * Returns the next <code>Node</code> a message should be routed to.
   *
   * @return the <code>Some(node)</code> to route the next message to or <code>None</code> if there are no <code>Node</code>s
   * available
   */
  def nextNode: Option[Node]
}

/**
 * A factory which can generate <code>LoadBalancer</code>s.
 */
trait LoadBalancerFactory {
  /**
   * Create a new load balancer instance based on the currently available <code>Node</code>s.
   *
   * @param nodes the currently available <code>Node</code>s in the cluster
   *
   * @return a new <code>LoadBalancer</code> instance
   * @throws InvalidClusterException thrown to indicate that the current cluster topology is invalid in some way and
   * it is impossible to create a <code>LoadBalancer</code>
   */
  @throws(classOf[InvalidClusterException])
  def newLoadBalancer(nodes: Set[Endpoint]): LoadBalancer
}

/**
 * A component which provides a <code>LoadBalancerFactory</code>.
 */
trait LoadBalancerFactoryComponent {
  val loadBalancerFactory: LoadBalancerFactory

}

trait LoadBalancerHelpers {
  import java.util.concurrent.atomic.AtomicInteger
  import math._

  def chooseNext[T](items: Seq[T], counter: AtomicInteger): T =
    items(abs(counter.getAndIncrement) % items.size)

}
