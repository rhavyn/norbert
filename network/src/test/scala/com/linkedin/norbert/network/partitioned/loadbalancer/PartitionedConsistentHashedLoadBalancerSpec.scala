package com.linkedin.norbert.network.partitioned.loadbalancer

import org.specs.Specification
import com.linkedin.norbert.network.common.Endpoint
import com.linkedin.norbert.cluster.{InvalidClusterException, Node}

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

class PartitionedConsistentHashedLoadBalancerSpec extends Specification {
  class TestLBF(numPartitions: Int, csr: Boolean = true)
          extends PartitionedConsistentHashedLoadBalancerFactory[Int](numPartitions,
            10,
            (id: Int) => HashFunctions.fnv(BigInt(id).toByteArray),
            (str: String) => str.hashCode(),
            csr)
  def toEndpoints(nodes: Set[Node]) = nodes.map(n => new Endpoint {
      def node = n

      def canServeRequests = true
    })

  val loadBalancerFactory = new TestLBF(5)

//  "DefaultPartitionedLoadBalancerFactory" should {
//    "return the correct partition id" in {
//      loadBalancerFactory.partitionForId(EId(1210)) must be_==(0)
//    }
//  }

  "ConsistentHashPartitionedLoadBalancer" should {
    "nextNode returns the correct node for 1210" in {
      val nodes = Set(
        Node(0, "localhost:31313", true, Set(0, 1)),
        Node(1, "localhost:31313", true, Set(1, 2)),
        Node(2, "localhost:31313", true, Set(2, 3)),
        Node(3, "localhost:31313", true, Set(3, 4)),
        Node(4, "localhost:31313", true, Set(0, 4)))

      val lb = loadBalancerFactory.newLoadBalancer(toEndpoints(nodes))
      lb.nextNode(1210) must beSome[Node].which(List(Node(0, "localhost:31313", true, Set(0, 1)),
        Node(4, "localhost:31313", true, Set(0, 4))) must contain(_))
    }


    "throw InvalidClusterException if all partitions are unavailable" in {
      val nodes = Set(
        Node(0, "localhost:31313", true, Set()),
        Node(1, "localhost:31313", true, Set()))

      new TestLBF(2, false).newLoadBalancer(toEndpoints(nodes)) must throwA[InvalidClusterException]
    }

    "throw InvalidClusterException if one partition is unavailable, and the LBF cannot serve requests in that state, " in {
      val nodes = Set(
        Node(0, "localhost:31313", true, Set(1)),
        Node(1, "localhost:31313", true, Set()))

      new TestLBF(2, true).newLoadBalancer(toEndpoints(nodes)) must not (throwA[InvalidClusterException])
      new TestLBF(2, false).newLoadBalancer(toEndpoints(nodes)) must throwA[InvalidClusterException]
    }
  }

}