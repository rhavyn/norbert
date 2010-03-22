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

/**
 * The default network client implementation component mixin. Users should mix this component into their
 * component registry instead of pulling in the individual components.
 */
//trait DefaultNetworkClientFactoryComponent extends NetworkClientFactoryComponent with NettyClusterIoClientComponent
//        with BootstrapFactoryComponent with ZooKeeperClusterClientComponent with NettyResponseHandlerComponent
//        with MessageRegistryComponent with ClientCurrentNodeLocatorComponent with MessageExecutorComponent {
//  this: RouterFactoryComponent =>
//
//  /**
//   * The maximum number of sockets that will be opened per node.
//   */
//  val maxConnectionsPerNode: Int
//
//  /**
//   * The maximum amount of time in milliseconds to allow a queued write request to sit before it should be considered timed out.
//   */
//  val writeTimeout: Int
//
//  private val clientMessageExecutor = new DoNothingMessageExecutor
//  def currentNodeLocator: CurrentNodeLocator = new ClientCurrentNodeLocator
//  def messageExecutor: MessageExecutor = clientMessageExecutor
//  val responseHandler = new NettyResponseHandler
//  val bootstrapFactory = new BootstrapFactory
//  val clusterIoClient = new NettyClusterIoClient(maxConnectionsPerNode, writeTimeout)
//  val networkClientFactory = new NetworkClientFactory
//}

/**
 * The default network server implementation component mixin. Users should mix this component into their
 * component registry instead of pulling in the individual components. This component includes the client
 * networking subsystem as well, so users writing a peer to peer application do not need to mixin in
 * the <code>DefaultNetworkClientFactoryComponent</code>.
 */
//trait DefaultNetworkServerComponent extends DefaultNetworkClientFactoryComponent with NettyNetworkServerComponent
//        with NettyRequestHandlerComponent with CurrentNodeLocatorComponent with MessageExecutorComponent with ServerCurrentNodeLocatorComponent {
//  this: RouterFactoryComponent =>
//
//  /**
//   * The core number of threads that are dedicated to handling requests.
//   */
//  val coreRequestThreadPoolSize: Int
//
//  /**
//   * The maximum number of threads that are dedicated to handling requests.
//   */
//  val maxRequestThreadPoolSize: Int
//
//  /**
//   * The amount of time an idle request handling thread should live in seconds.
//   */
//  val requestThreadTimeout: Int
//
//  private val serverMessageExecutor = new ThreadPoolMessageExecutor(coreRequestThreadPoolSize, maxRequestThreadPoolSize, requestThreadTimeout)
//  override def currentNodeLocator = new ServerCurrentNodeLocator
//  override def messageExecutor: MessageExecutor = serverMessageExecutor
//  val requestHandler = new NettyRequestHandler
//}
