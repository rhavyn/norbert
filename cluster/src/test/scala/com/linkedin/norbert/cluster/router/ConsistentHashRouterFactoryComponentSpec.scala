/*
 * Copyright 2009 LinkedIn, Inc
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
package com.linkedin.norbert.cluster.router

import org.specs.SpecificationWithJUnit
import com.linkedin.norbert.cluster.{InvalidClusterException, Node}
import java.net.InetSocketAddress

class ConsistentHashRouterFactoryComponentSpec extends SpecificationWithJUnit
        with ConsistentHashRouterFactoryComponent {
  case class EId(id: Int)
  implicit def eId2ByteArray(eId: EId): Array[Byte] = BigInt(eId.id).toByteArray

  type Id = EId
  class EIdConsistentHashRouterFactory(numPartitions: Int) extends ConsistentHashRouterFactory(numPartitions) {
    protected def calculateHash(id: EId) = HashFunctions.fnv(id)
  }

  val routerFactory = new EIdConsistentHashRouterFactory(5)

  "RouterFactory" should {
    "return the correct partition id" in {
      routerFactory.partitionForId(EId(1210)) must be_==(0)
    }
  }

  "Router" should {
    "route returns the correct node for 1210" in {
      val nodes = Array(
        Node(0, new InetSocketAddress("localhost", 1), Array(0, 1), true),
        Node(1, new InetSocketAddress("localhost", 1), Array(1, 2), true),
        Node(2, new InetSocketAddress("localhost", 1), Array(2, 3), true),
        Node(3, new InetSocketAddress("localhost", 1), Array(3, 4), true),
        Node(4, new InetSocketAddress("localhost", 1), Array(0, 4), true))

      val route = routerFactory.newRouter(nodes)
      route(EId(1210)) must beSome[Node].which(Array(nodes(0), nodes(4)) must contain(_))
    }

    "throw InvalidClusterException if a partition is not assigned to a node" in {
      val nodes = Array(
        Node(0, new InetSocketAddress("localhost", 1), Array(0, 9), true),
        Node(1, new InetSocketAddress("localhost", 1), Array(1), true),
        Node(2, new InetSocketAddress("localhost", 1), Array(2, 7), true),
        Node(3, new InetSocketAddress("localhost", 1), Array(3, 6), true),
        Node(4, new InetSocketAddress("localhost", 1), Array(4, 5, 1), true))

      new EIdConsistentHashRouterFactory(10).newRouter(nodes) must throwA[InvalidClusterException]
    }
  }
}
