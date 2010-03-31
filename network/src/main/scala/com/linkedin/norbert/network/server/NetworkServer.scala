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
package com.linkedin.norbert.network.server

import com.linkedin.norbert.cluster._
import java.util.concurrent.atomic.AtomicBoolean
import com.linkedin.norbert.util.Logging
import com.linkedin.norbert.network.{NetworkShutdownException, NetworkingException, NetworkServerNotBoundException}
import com.google.protobuf.Message
import com.linkedin.norbert.network.netty.{NettyNetworkServer, NetworkServerConfig}

object NetworkServer {
  def apply(config: NetworkServerConfig): NetworkServer = new NettyNetworkServer(config)
}

trait NetworkServer extends Logging {
  this: ClusterClientComponent with ClusterIoServerComponent with MessageHandlerRegistryComponent =>

  @volatile private var markAvailableWhenConnected = true
  private var listenerKey: ClusterListenerKey = _
  private var nodeOption: Option[Node] = None
  private val shutdownSwitch = new AtomicBoolean

  /**
   * Registers a message handler with the <code>NetworkServer</code>. The <code>NetworkServer</code> will call the
   * provided handler when an incoming request of type <code>requestMessage</code> is received.  If a response is
   * expected then a response message should also be provided.
   *
   * @param requestMessage an instance of an incoming request message
   * @param responseMessage an instance of an outgoing response message
   * @param handler the function to call when an incoming message of type <code>requestMessage</code> is recieved
   */
  def registerHandler(requestMessage: Message, responseMessage: Message, handler: (Message) => Message) {
    messageHandlerRegistry.registerHandler(requestMessage, responseMessage, handler)
  }

  /**
   * Binds the network server instance to the wildcard address and the port of the <code>Node</code> identified
   * by the provided nodeId and automatically marks the <code>Node</code> available in the cluster.  A
   * <code>Node</code>'s url must be specified in the format hostname:port.
   *
   * @param nodeId the id of the <code>Node</code> this server is associated with.
   *
   * @throws InvalidNodeException thrown if no <code>Node</code> with the specified <code>nodeId</code> exists
   * @throws NetworkingException thrown if unable to bind
   */
  def bind(nodeId: Int): Unit = bind(nodeId, true)

  /**
   * Binds the network server instance to the wildcard address and the port of the <code>Node</code> identified
   * by the provided nodeId and marks the <code>Node</code> available in the cluster if <code>markAvailable</code> is true.  A
   * <code>Node</code>'s url must be specified in the format hostname:port.
   *
   * @param nodeId the id of the <code>Node</code> this server is associated with.
   * @param markAvailable if true marks the <code>Node</code> identified by <code>nodeId</code> as available after binding to
   * the port
   *
   * @throws InvalidNodeException thrown if no <code>Node</code> with the specified <code>nodeId</code> exists or if the
   * format of the <code>Node</code>'s url isn't hostname:port
   * @throws NetworkingException thrown if unable to bind
   */
  def bind(nodeId: Int, markAvailable: Boolean): Unit = doIfNotShutdown {
    if (nodeOption.isDefined) throw new NetworkingException("Attempt to bind an already bound NetworkServer")

    log.ifInfo("Starting NetworkServer...")

    log.ifDebug("Ensuring ClusterClient is started")
    clusterClient.start
    clusterClient.awaitConnectionUninterruptibly

    val node = clusterClient.nodeWithId(nodeId).getOrElse(throw new InvalidNodeException("No node with id %d exists".format(nodeId)))
    clusterIoServer.bind(node, true)

    nodeOption = Some(node)
    markAvailableWhenConnected = markAvailable

    log.ifDebug("Registering with ClusterClient")
    listenerKey = clusterClient.addListener(new ClusterListener {
      def handleClusterEvent(event: ClusterEvent) = event match {
        case ClusterEvents.Connected(_) => if (markAvailableWhenConnected) clusterClient.markNodeAvailable(nodeId)
        case ClusterEvents.Shutdown => doShutdown(true)
        case _ => // do nothing
      }
    })

    log.ifInfo("NetworkServer started")
  }

  /**
   * Returns the <code>Node</code> associated with this server.
   *
   * @return the <code>Node</code> associated with this server
   */
  def myNode: Node = doIfNotShutdown { nodeOption.getOrElse(throw new NetworkServerNotBoundException) }

  /**
   * Marks the node available in the cluster if the server is bound.
   */
  def markAvailable: Unit = {
    clusterClient.markNodeAvailable(myNode.id)
    markAvailableWhenConnected = true
  }

  /**
   * Marks the node unavailable in the cluster if bound.
   */
  def markUnavailable: Unit = {
    clusterClient.markNodeUnavailable(myNode.id)
    markAvailableWhenConnected = false
  }

  /**
   * Shuts down the network server. This results in unbinding from the port, closing the child sockets, and marking the node unavailable.
   */
  def shutdown: Unit = doShutdown(false)

  private def doShutdown(fromCluster: Boolean) {
    if (shutdownSwitch.compareAndSet(false, true)) {
      log.ifInfo("Shutting down NetworkServer for %s...", nodeOption.map(_.toString).getOrElse("[unbound]"))

      if (!fromCluster) {
        nodeOption.foreach { node =>
          try {
            log.ifDebug("Unregistering from ClusterClient")
            clusterClient.removeListener(listenerKey)

            log.ifDebug("Marking %s unavailable", node)
            clusterClient.markNodeUnavailable(node.id)
          } catch {
            case ex: ClusterShutdownException => // cluster already shut down, ignore
          }
        }
      }

      log.ifDebug("Closing opened sockets")
      clusterIoServer.shutdown

      log.ifInfo("NetworkServer shut down")
    }
  }

  private def doIfNotShutdown[T](block: => T): T = if (shutdownSwitch.get) throw new NetworkShutdownException else block
}
