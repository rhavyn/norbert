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
package com.linkedin.norbert.network

import netty.{NettyRequestHandlerComponent, BootstrapFactoryComponent}
import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import com.linkedin.norbert.cluster._
import loadbalancer.RouterFactoryComponent

//class ServerCurrentNodeLocatorComponentSpec extends SpecificationWithJUnit with Mockito with ServerCurrentNodeLocatorComponent
//        with NetworkServerComponent with BootstrapFactoryComponent with ClusterClientComponent with NetworkClientFactoryComponent
//        with ClusterManagerComponent with RouterFactoryComponent with NettyRequestHandlerComponent
//        with ClusterIoClientComponent with MessageRegistryComponent
//        with ResponseHandlerComponent with MessageExecutorComponent {
//  val clusterClient = null
//  val networkClientFactory = null
//  val bootstrapFactory = null
//  val routerFactory = null
//  val requestHandler = null
//  val messageRegistry = null
//  val responseHandler = null
//  val messageExecutor = null
//
//  val currentNodeLocator = new ServerCurrentNodeLocator
//  val networkServer = mock[NetworkServer]
//
//  "CurrentNodeLocator" should {
//    "return the current node from the server" in {
//      val node = Node(1, "localhost:31313", Array(0, 1), false)
//      networkServer.currentNode returns node
//
//      currentNodeLocator.currentNode must be(node)
//    }
//  }
//}
