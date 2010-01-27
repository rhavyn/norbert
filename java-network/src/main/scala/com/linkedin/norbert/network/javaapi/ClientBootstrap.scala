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
import com.linkedin.norbert.cluster.javaapi.{ClusterConfig, JavaRouterHelper}
import scala.reflect.BeanProperty
import com.linkedin.norbert.NorbertException
import com.linkedin.norbert.network.{NetworkDefaults, DefaultNetworkClientFactoryComponent}

/**
 * JavaBean which provides configuration properties exposed by <code>ClientBootstrap</code>.
 */
class ClientConfig extends ClusterConfig {

  /**
   * The amount of time a write should be queued before it is considered timed out in milliseconds. Default is
   * 150ms.
   */
  @BeanProperty var writeTimeout = NetworkDefaults.WRITE_TIMEOUT

  /**
   * The maximum number of connections that can be opened per node. Default is 20.
   */
  @BeanProperty var maxConnectionsPerNode = NetworkDefaults.MAX_CONNECTIONS_PER_NODE

  /**
   * The messages which the client expects to receive as responses to outgoing requests.
   */
  @BeanProperty var responseMessages: Array[Message] = _

  @throws(classOf[NorbertException])
  override def validate() = {
    super.validate
    if (responseMessages == null) throw new NorbertException("responseMessages must be specified")
  }
}

/**
 * A bootstrap for creating <code>NetworkClient</code> instances.
 *
 * @param clientConfig the <code>ClientConfig</code> to use to configure the new instances
 *
 * @throws NorbertException thrown if the <code>ClusterConfig</code> provided is not valid
 */
@throws(classOf[NorbertException])
class ClientBootstrap(clientConfig: ClientConfig) extends ClientBootstrapHelper {
  clientConfig.validate
  
  protected object componentRegistry extends {
    val clusterName = clientConfig.clusterName
    val zooKeeperUrls = clientConfig.zooKeeperUrls
    val javaRouterFactory = clientConfig.routerFactory
    val clusterDisconnectTimeout = clientConfig.clusterDisconnectTimeout
    val zooKeeperSessionTimeout = clientConfig.zooKeeperSessionTimeout
    val writeTimeout = clientConfig.writeTimeout
    val maxConnectionsPerNode = clientConfig.maxConnectionsPerNode
  } with DefaultNetworkClientFactoryComponent with JavaRouterHelper {
    val messageRegistry = new DefaultMessageRegistry(clientConfig.responseMessages)
  }
}
