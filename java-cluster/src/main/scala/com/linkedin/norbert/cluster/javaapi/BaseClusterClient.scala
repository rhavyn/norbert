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
package com.linkedin.norbert.cluster.javaapi

import java.lang.String
import java.util.concurrent.TimeUnit
import com.linkedin.norbert.cluster.{ClusterEvents, ClusterEvent, ClusterListenerKey}
import Implicits._

abstract class BaseClusterClient extends ClusterClient {

  val underlying: com.linkedin.norbert.cluster.ClusterClient

  def shutdown = underlying.shutdown

  def awaitConnectionUninterruptibly = underlying.awaitConnectionUninterruptibly

  def awaitConnection(timeout: Long, unit: TimeUnit) = underlying.awaitConnection(timeout, unit)

  def awaitConnection = underlying.awaitConnection

  def isShutdown = underlying.isShutdown

  def isConnected = underlying.isConnected

  def removeListener(key: ClusterListenerKey) = underlying.removeListener(key)

  def addListener(listener: ClusterListener) = underlying.addListener(new com.linkedin.norbert.cluster.ClusterListener {
    import ClusterEvents._

    def handleClusterEvent(event: ClusterEvent) = event match {
      case Connected(nodes) => listener.handleClusterConnected(nodes)
      case NodesChanged(nodes) => listener.handleClusterNodesChanged(nodes)
      case Disconnected => listener.handleClusterDisconnected
      case Shutdown => listener.handleClusterShutdown
    }
  })

  def markNodeUnavailable(nodeId: Int) = underlying.markNodeUnavailable(nodeId)

  def markNodeAvailable(nodeId: Int) = underlying.markNodeAvailable(nodeId)

  def removeNode(nodeId: Int) = underlying.removeNode(nodeId)

  def addNode(nodeId: Int, url: String, partitions: java.util.Set[java.lang.Integer]) = {
    underlying.addNode(nodeId, url, partitions.asInstanceOf[java.util.Set[Int]])
  }

  def addNode(nodeId: Int, url: String) = underlying.addNode(nodeId, url)

  def getNodeWithId(nodeId: Int) = underlying.nodeWithId(nodeId).getOrElse(null)

  def getNodes = underlying.nodes

  def getServiceName = underlying.serviceName
}
