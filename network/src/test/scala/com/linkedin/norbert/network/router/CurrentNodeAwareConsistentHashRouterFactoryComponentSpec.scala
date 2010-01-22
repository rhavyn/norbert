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
package com.linkedin.norbert.network.router

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import com.linkedin.norbert.network.CurrentNodeLocatorComponent
import com.linkedin.norbert.cluster.router.HashFunctions
import java.net.InetSocketAddress
import com.linkedin.norbert.cluster.Node

class CurrentNodeAwareConsistentHashRouterFactoryComponentSpec extends SpecificationWithJUnit with Mockito
        with CurrentNodeAwareConsistentHashRouterFactoryComponent with CurrentNodeLocatorComponent {
  case class EId(id: Int)
  implicit def eId2ByteArray(eId: EId): Array[Byte] = BigInt(eId.id).toByteArray

  type Id = EId
  class EIdCurrentNodeAwareConsistentHashRouterFactory(numPartitions: Int) extends CurrentNodeAwareConsistentHashRouterFactory(numPartitions) {
    protected def calculateHash(id: EId) = HashFunctions.fnv(id)
  }

  val currentNodeLocator = mock[CurrentNodeLocator]
  val routerFactory = new EIdCurrentNodeAwareConsistentHashRouterFactory(5)

  "Router" should {
    "route to the correct node if CurrentNodeLocator returns null" in {
      currentNodeLocator.currentNode returns null
      val nodes = Array(
        Node(0, new InetSocketAddress("localhost", 1), Array(0, 1), true),
        Node(1, new InetSocketAddress("localhost", 1), Array(1, 2), true),
        Node(2, new InetSocketAddress("localhost", 1), Array(2, 3), true),
        Node(3, new InetSocketAddress("localhost", 1), Array(3, 4), true),
        Node(4, new InetSocketAddress("localhost", 1), Array(0, 4), true))

      val route = routerFactory.newRouter(nodes)
      route(EId(1210)) must beSome[Node].which(Array(nodes(0), nodes(4)) must contain(_))
    }

    "route to the current node" in {
      val nodes = Array(
        Node(0, new InetSocketAddress("localhost", 1), Array(0, 1), true),
        Node(1, new InetSocketAddress("localhost", 1), Array(1, 2), true),
        Node(2, new InetSocketAddress("localhost", 1), Array(2, 3), true),
        Node(3, new InetSocketAddress("localhost", 1), Array(3, 4), true),
        Node(4, new InetSocketAddress("localhost", 1), Array(0, 4), true))
      currentNodeLocator.currentNode returns nodes(4)

      val routerFactory = new EIdCurrentNodeAwareConsistentHashRouterFactory(5)
      val route = routerFactory.newRouter(nodes)
      for (i <- 0 to 20) {
        route(EId(1210)) must beSome[Node].which(_ == nodes(4))
      }
    }

    "not route to the current node" in {
      val nodes = Array(
        Node(0, new InetSocketAddress("localhost", 1), Array(0, 1), true),
        Node(1, new InetSocketAddress("localhost", 1), Array(1, 2), true),
        Node(2, new InetSocketAddress("localhost", 1), Array(2, 3), true),
        Node(3, new InetSocketAddress("localhost", 1), Array(3, 4), true),
        Node(4, new InetSocketAddress("localhost", 1), Array(0, 4), true))
      currentNodeLocator.currentNode returns nodes(2)

      val routerFactory = new EIdCurrentNodeAwareConsistentHashRouterFactory(5)
      val route = routerFactory.newRouter(nodes)
      for (i <- 0 to 20) {
        route(EId(1210)) must beSome[Node].which(_ != nodes(2))
      }
    }
  }
}
