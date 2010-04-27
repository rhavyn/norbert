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

import org.specs.SpecificationWithJUnit
import com.linkedin.norbert.cluster.{InvalidClusterException, Node}

class ConsistentHashPartitionedLoadBalancerFactorySpec extends SpecificationWithJUnit {
  case class EId(id: Int)
  implicit def eId2ByteArray(eId: EId): Array[Byte] = BigInt(eId.id).toByteArray

  class EIdConsistentHashLoadBalancerFactory(numPartitions: Int) extends ConsistentHashPartitionedLoadBalancerFactory[EId](numPartitions) {
    protected def calculateHash(id: EId) = HashFunctions.fnv(id)
  }

  val loadBalancerFactory = new EIdConsistentHashLoadBalancerFactory(5)

  "ConsistentHashPartitionedLoadBalancerFactory" should {
    "return the correct partition id" in {
      loadBalancerFactory.partitionForId(EId(1210)) must be_==(0)
    }
  }

  "ConsistentHashPartitionedLoadBalancer" should {
    "nextNode returns the correct node for 1210" in {
      val nodes = Set(
        Node(0, "localhost:31313", Set(0, 1), true),
        Node(1, "localhost:31313", Set(1, 2), true),
        Node(2, "localhost:31313", Set(2, 3), true),
        Node(3, "localhost:31313", Set(3, 4), true),
        Node(4, "localhost:31313", Set(0, 4), true))

      val lb = loadBalancerFactory.newLoadBalancer(nodes)
      lb.nextNode(EId(1210)) must beSome[Node].which(Array(Node(0, "localhost:31313", Set(0, 1), true),
        Node(4, "localhost:31313", Set(0, 4), true)) must contain(_))
    }

    "throw InvalidClusterException if a partition is not assigned to a node" in {
      val nodes = Set(
        Node(0, "localhost:31313", Set(0, 9), true),
        Node(1, "localhost:31313", Set(1), true),
        Node(2, "localhost:31313", Set(2, 7), true),
        Node(3, "localhost:31313", Set(3, 6), true),
        Node(4, "localhost:31313", Set(4, 5, 1), true))

      new EIdConsistentHashLoadBalancerFactory(10).newLoadBalancer(nodes) must throwA[InvalidClusterException]
    }
  }
}
