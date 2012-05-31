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
package com.linkedin.norbert.javacompat
package network

import cluster.BaseClusterClient
import com.linkedin.norbert.cluster.ClusterClient
import com.linkedin.norbert.network.Serializer

class NettyNetworkServer(config: NetworkServerConfig) extends NetworkServer {
  val c = new com.linkedin.norbert.network.netty.NetworkServerConfig

  c.clusterClient = if (config.getClusterClient != null)
    config.getClusterClient.asInstanceOf[BaseClusterClient].underlying
  else ClusterClient(null, config.getServiceName, config.getZooKeeperConnectString, config.getZooKeeperSessionTimeoutMillis)
  
  c.zooKeeperSessionTimeoutMillis = config.getZooKeeperSessionTimeoutMillis
  c.requestThreadCorePoolSize = config.getRequestThreadCorePoolSize
  c.requestThreadMaxPoolSize = config.getRequestThreadMaxPoolSize
  c.requestThreadKeepAliveTimeSecs = config.getRequestThreadKeepAliveTimeSecs

  val underlying = com.linkedin.norbert.network.server.NetworkServer(c)

  def shutdown = underlying.shutdown

  def markUnavailable = underlying.markUnavailable

  def markAvailable = underlying.markAvailable

  def getMyNode = underlying.myNode

  def bind(nodeId: Int, markAvailable: Boolean) = underlying.bind(nodeId, markAvailable)

  def bind(nodeId: Int) = underlying.bind(nodeId)

  def registerHandler[RequestMsg, ResponseMsg](handler: RequestHandler[RequestMsg, ResponseMsg], serializer: Serializer[RequestMsg, ResponseMsg]) = {
    underlying.registerHandler((request: RequestMsg) => handler.handleRequest(request))(serializer, serializer)
  }
}
