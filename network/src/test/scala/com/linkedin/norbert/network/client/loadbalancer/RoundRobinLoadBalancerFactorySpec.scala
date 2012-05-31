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

import org.specs.Specification
import cluster.Node
import common.Endpoint

class RoundRobinLoadBalancerFactorySpec extends Specification {
  "RoundRobinLoadBalancerFactory" should {
    "create a round robin load balancer" in {
      val nodes = Set(Node(1, "localhost:31310", true), Node(2, "localhost:31311", true), Node(3, "localhost:31312", true),
        Node(4, "localhost:31313", true), Node(5, "localhost:31314", true), Node(6, "localhost:31315", true),
        Node(7, "localhost:31316", true), Node(8, "localhost:31317", true), Node(9, "localhost:31318", true),
        Node(10, "localhost:31319", true))
      val loadBalancerFactory = new RoundRobinLoadBalancerFactory

      val endpoints = nodes.map(n => new Endpoint {
        def node = n

        def canServeRequests = true
      })

      val lb = loadBalancerFactory.newLoadBalancer(endpoints)

      for (i <- 0 until 100) {
        val node = lb.nextNode
        node must beSome[Node].which { nodes must contain(_) }
      }
    }

    "Not route to offline endpoints" in {
      val nodes = for(i <- 0 until 3) yield Node(i, "localhost:3131" + i, true)

      var availabilityMap = nodes.map(n => (n, true)).toMap

      val lbf = new RoundRobinLoadBalancerFactory

      val endpoints = nodes.map(n => new Endpoint {
        def node = n

        def canServeRequests = availabilityMap(n)
      }).toSet

      var lb = lbf.newLoadBalancer(endpoints)

      for(i <- 0 until 9) {
        val node = lb.nextNode.get
        val nodeId = i % 3
        node must be_==(Node(nodeId, "localhost:3131" + nodeId, true))
      }

      availabilityMap += Node(0, "localhost:31310", true) -> false

      lb = lbf.newLoadBalancer(endpoints)
      for(i <- 0 until 9) {
        val node = lb.nextNode.get
        val nodeId = i % 2 + 1
        node must be_==(Node(nodeId, "localhost:3131" + nodeId, true))
      }
    }
  }
}
