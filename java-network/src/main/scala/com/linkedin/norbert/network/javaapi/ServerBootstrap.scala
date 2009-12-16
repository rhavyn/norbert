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
package com.linkedin.norbert.network.javaapi

import com.linkedin.norbert.network.DefaultNetworkServerComponent
import com.google.protobuf.Message
import com.linkedin.norbert.cluster.javaapi.JavaRouterHelper
import com.linkedin.norbert.network.javaapi.{NetworkServer => JNetworkServer}
import java.net.InetSocketAddress
import scala.reflect.BeanProperty
import com.linkedin.norbert.NorbertException

class ServerConfig extends ClientConfig {
  @BeanProperty var messageHandlers: Array[MessageHandler] = _
  @BeanProperty var nodeId: Int = -1
  @BeanProperty var bindAddress: InetSocketAddress = _

  override def validate() = {
    if (clusterName == null || zooKeeperUrls == null || messageHandlers == null ||
            (nodeId == -1 && bindAddress == null)) throw new NorbertException("clusterName, zooKeeperUrls, requestMessages and either nodeId or bindAddress must be specified")
  }
}

class ServerBootstrap(serverConfig: ServerConfig) extends ClientBootstrapHelper {
  serverConfig.validate
  
  protected object componentRegistry extends {
    val clusterName = serverConfig.clusterName
    val zooKeeperUrls = serverConfig.zooKeeperUrls
    val javaRouterFactory = serverConfig.routerFactory

    override val clusterDisconnectTimeout = serverConfig.clusterDisconnectTimeout
    override val zooKeeperSessionTimeout = serverConfig.zooKeeperSessionTimeout
  } with DefaultNetworkServerComponent with JavaRouterHelper {
    val messageRegistry = new DefaultMessageRegistry(serverConfig.responseMessages, for {
      mh <- serverConfig.messageHandlers
      m <- mh.getMessages
    } yield (m, (message: Message) => mh.handleMessage(message) match {
      case null => None
      case msg => Some(msg)
    }))

    val networkServer = if (serverConfig.bindAddress == null) {
      new NetworkServer(serverConfig.bindAddress)
    } else {
      new NetworkServer(serverConfig.nodeId)
    }
  }

  import componentRegistry.NetworkServer
  
  def getNetworkServer: JNetworkServer = new NetworkServerWrapper(componentRegistry.networkServer)

  private class NetworkServerWrapper(networkServer: NetworkServer) extends JNetworkServer {
    def bind = networkServer.bind

    def getCurrentNode = networkServer.currentNode

    def shutdown = networkServer.shutdown
  }
}
