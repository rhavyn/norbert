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

import com.linkedin.norbert.cluster.{RouterFactoryComponent, DefaultClusterComponent}
import netty._

trait DefaultNetworkClientFactoryComponent extends NetworkClientFactoryComponent with NettyClusterIoClientComponent
        with BootstrapFactoryComponent with DefaultClusterComponent with RequestHandlerComponent
        with MessageRegistryComponent {
  this: RouterFactoryComponent =>

  val maxConnectionsPerNode: Int
  val writeTimeout: Int

  val bootstrapFactory = new BootstrapFactory
  val clusterIoClient = new NettyClusterIoClient(maxConnectionsPerNode, writeTimeout)
  val networkClientFactory = new NetworkClientFactory
}

trait DefaultNetworkServerComponent extends DefaultNetworkClientFactoryComponent with NettyNetworkServerComponent
        with ChannelHandlerActorComponent with MessageHandlerComponent with CurrentNodeLocatorComponent {
  this: RouterFactoryComponent =>
  
  val messageHandler = new MessageHandler
  val currentNodeLocator = new CurrentNodeLocator
}
