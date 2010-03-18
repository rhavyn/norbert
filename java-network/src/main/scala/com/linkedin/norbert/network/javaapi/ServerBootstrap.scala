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
package com.linkedin.norbert.network.javaapi

import com.google.protobuf.Message
import com.linkedin.norbert.cluster.javaapi.JavaRouterHelper
import com.linkedin.norbert.network.javaapi.{NetworkServer => JNetworkServer}
import java.net.InetSocketAddress
import scala.reflect.BeanProperty
import com.linkedin.norbert.NorbertException
import com.linkedin.norbert.network.{NetworkDefaults, DefaultNetworkServerComponent}

/**
 * JavaBean which provides configuration properties exposed by <code>ServerBootstrap</code>.
 */
class ServerConfig extends ClientConfig {

  /**
   * The <code>MessageHandler</code>s to use to process incoming requests.
   */
  @BeanProperty var messageHandlers: Array[MessageHandler] = _

  /**
   * The id of the node for which the <code>NetworkServer</code> is handling requests. The <code>NetworkServer</code>
   * will look up the node in ZooKeeper and attempt to bind to the host/port registered for that node.  Only one
   * of nodeId and bindAddress should be specified.
   */
  @BeanProperty var nodeId: Int = -1

  /**
   * The address to which the <code>NetworkServer</code> to bind.  The <code>NetworkServer</code> will attempt
   * to find a node with the same address as this (using the method ClusterClient.getNodeWithAddress) and will
   * consider that node as the current node. Only one of nodeId and bindAddress should be specified.
   */
  @BeanProperty var bindAddress: InetSocketAddress = _

  /**
   * The amount of time a idle request thread should be allowed to live in seconds.  Default is 300s.
   */
  @BeanProperty var requestThreadTimeout = NetworkDefaults.REQUEST_THREAD_TIMEOUT

  /**
   * The core number of request threads to maintain. Default is 2 * the number of processors.
   */
  @BeanProperty var coreRequestThreadPoolSize = NetworkDefaults.CORE_REQUEST_THREAD_POOL_SIZE

  /**
   * The maximum number of request threads to create. Default is 5 * coreRequestThreadPoolSize.
   */
  @BeanProperty var maxRequestThreadPoolSize = NetworkDefaults.MAX_REQUEST_THREAD_POOL_SIZE

  @throws(classOf[NorbertException])
  override def validate() = {
    if (clusterName == null || zooKeeperUrls == null || messageHandlers == null ||
            (nodeId == -1 && bindAddress == null)) throw new NorbertException("clusterName, zooKeeperUrls, requestMessages and either nodeId or bindAddress must be specified")
  }
}

/**
 * A bootstrap for creating a <code>NetworkServer</code> instance.  Additionally, for peer to peer
 * systems, <code>NetworkClient</code> instances can also be created.
 *
 * @param serverConfig the <code>ServerConfig</code> to use to configure the new instance
 *
 * @throws NorbertException thrown if the <code>ServerConfig</code> provided is not valid
 */
@throws(classOf[NorbertException])
class ServerBootstrap(serverConfig: ServerConfig) extends ClientBootstrapHelper {
  serverConfig.validate

  protected object componentRegistry extends {
    val clusterName = serverConfig.clusterName
    val zooKeeperConnectString = serverConfig.zooKeeperUrls
    val javaRouterFactory = serverConfig.routerFactory
    val clusterDisconnectTimeout = serverConfig.clusterDisconnectTimeout
    val zooKeeperSessionTimeout = serverConfig.zooKeeperSessionTimeout
    val writeTimeout = serverConfig.writeTimeout
    val maxConnectionsPerNode = serverConfig.maxConnectionsPerNode
    val requestThreadTimeout = serverConfig.requestThreadTimeout
    val maxRequestThreadPoolSize = serverConfig.maxRequestThreadPoolSize
    val coreRequestThreadPoolSize = serverConfig.coreRequestThreadPoolSize
  } with DefaultNetworkServerComponent with JavaRouterHelper {
    val messageRegistry = new DefaultMessageRegistry(serverConfig.responseMessages, for {
      mh <- serverConfig.messageHandlers
      m <- mh.getMessages
    } yield (m, (message: Message) => mh.handleMessage(message) match {
      case null => None
      case msg => Some(msg)
    }))

    val networkServer = if (serverConfig.bindAddress == null) {
      new NettyNetworkServer(serverConfig.nodeId)
    } else {
      new NettyNetworkServer(serverConfig.bindAddress)
    }
  }

  import componentRegistry.NetworkServer

  /**
   * Retrieves the <code>NetworkServer</code> instance created by the bootstrap.
   *
   * @return a <code>NetworkServer</code> instance
   */
  def getNetworkServer: JNetworkServer = new NetworkServerWrapper(componentRegistry.networkServer)

  private class NetworkServerWrapper(networkServer: NetworkServer) extends JNetworkServer {
    def bind = networkServer.bind

    def bind(markAvailable: Boolean) = networkServer.bind(markAvailable)

    def getCurrentNode = networkServer.currentNode

    def markAvailable = networkServer.markAvailable

    def shutdown = networkServer.shutdown
  }
}
