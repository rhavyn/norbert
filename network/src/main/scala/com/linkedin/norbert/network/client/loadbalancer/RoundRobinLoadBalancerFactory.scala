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

import common.Endpoint
import cluster.Node
import java.util.concurrent.atomic.AtomicInteger
import annotation.tailrec

class RoundRobinLoadBalancerFactory extends LoadBalancerFactory {
  def newLoadBalancer(endpoints: Set[Endpoint]): LoadBalancer = new LoadBalancer {
    val MAX_ITERATIONS = endpoints.size * 4

    val counter = new AtomicInteger(0)
    val indexedNodes = endpoints.toArray

    def nextNode = nextNode(0)

    @tailrec
    private final def nextNode(numIterations: Int): Option[Node] = {
      val index = counter.getAndIncrement % indexedNodes.length
      val endpoint = indexedNodes(index)

      if(endpoint.canServeRequests)
        Some(endpoint.node)
      else if(numIterations > MAX_ITERATIONS)
        None
      else nextNode(numIterations + 1)
    }
  }
}
