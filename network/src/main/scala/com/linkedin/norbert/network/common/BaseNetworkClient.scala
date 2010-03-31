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
package com.linkedin.norbert.network.common

import com.linkedin.norbert.logging.Logging
import com.google.protobuf.Message
import com.linkedin.norbert.cluster._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.Future
import com.linkedin.norbert.network.{NetworkShutdownException, InvalidMessageException, ResponseIterator, NetworkNotStartedException}

trait BaseNetworkClient extends Logging {
  this: ClusterClientComponent with ClusterIoClientComponent with MessageRegistryComponent =>

  @volatile protected var currentNodes: Seq[Node] = Nil
  @volatile protected var connected = false
  protected val startedSwitch = new AtomicBoolean
  protected val shutdownSwitch = new AtomicBoolean

  private var listenerKey: ClusterListenerKey = _

  def start {
    if (startedSwitch.compareAndSet(false, true)) {
      log.ifDebug("Ensuring cluster is started")
      clusterClient.start
      clusterClient.awaitConnectionUninterruptibly
      updateCurrentState(clusterClient.nodes)
      connected = clusterClient.isConnected

      listenerKey = clusterClient.addListener(new ClusterListener {
        def handleClusterEvent(event: ClusterEvent) = event match {
          case ClusterEvents.Connected(nodes) =>
            updateCurrentState(nodes)
            connected = true

          case ClusterEvents.NodesChanged(nodes) => updateCurrentState(nodes)

          case ClusterEvents.Disconnected =>
            connected = false
            updateCurrentState(Nil)

          case ClusterEvents.Shutdown => doShutdown(true)
        }
      })
    }
  }

  /**
   * Registers a request/response message pair with the <code>NetworkClient</code>.  Requests and their associated
   * responses must be registered or an <code>InvalidMessageException</code> will be thrown when an attempt to send
   * a <code>Message</code> is made.
   *
   * @param requestMessage an instance of an outgoing request message
   * @param responseMessage an instance of the expected response message or null if this is a one way message
   */
  def registerRequest(requestMessage: Message, responseMessage: Message) {
    messageRegistry.registerMessage(requestMessage, responseMessage)
  }

  /**
   * Sends a message to the specified node in the cluster.
   *
   * @param message the message to send
   * @param node the node to send the message to
   *
   * @return a future which will become available when a response to the message is received
   * @throws InvalidNodeException thrown if the node specified is not currently available
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   */
  def sendMessageToNode(message: Message, node: Node): Future[Message] = doIfConnected {
    if (message == null || node == null) throw new NullPointerException
    verifyMessageRegistered(message)

    val candidate = currentNodes.filter(_ == node)
    if (candidate.length == 0) throw new InvalidNodeException("Unable to send message, %s is not available".format(node))

    val future = new NorbertFuture
    doSendMessage(node, message, e => future.offerResponse(e))

    future
  }

  /**
   * Broadcasts a message to all the currently available nodes in the cluster.
   *
   * @param message the message to send
   *
   * @return a <code>ResponseIterator</code> which will provide the responses from the nodes in the cluster
   * as they are received
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   */
  def broadcastMessage(message: Message): ResponseIterator = doIfConnected {
    if (message == null) throw new NullPointerException
    verifyMessageRegistered(message)

    val nodes = currentNodes
    val it = new NorbertResponseIterator(nodes.length)

    currentNodes.foreach(doSendMessage(_, message, e => it.offerResponse(e)))

    it
  }

  /**
   * Shuts down the <code>NetworkClient</code> and releases resources held.
   */
  def shutdown: Unit = doShutdown(false)

  protected def updateLoadBalancer(nodes: Seq[Node]): Unit

  protected def verifyMessageRegistered(message: Message) {
    if (!messageRegistry.contains(message)) throw new InvalidMessageException("The message provided [%s] is not a registered request message".format(message))
  }

  protected def doSendMessage(node: Node, message: Message, responseHandler: (Either[Throwable, Message]) => Unit) {
    clusterIoClient.sendMessage(node, message, responseHandler)
  }

  protected def doIfConnected[T](block: => T): T = {
    if (shutdownSwitch.get) throw new NetworkShutdownException
    else if (!startedSwitch.get) throw new NetworkNotStartedException
    else if (!connected) throw new ClusterDisconnectedException
    else block
  }

  private def updateCurrentState(nodes: Seq[Node]) {
    // TODO: If a node goes away, it should be removed from the ClusterIoClient
    currentNodes = nodes
    updateLoadBalancer(nodes)
  }

  private def doShutdown(fromCluster: Boolean) {
    if (shutdownSwitch.compareAndSet(false, true) && startedSwitch.get) {
      log.ifInfo("Shutting down NetworkClient")

      if (!fromCluster) {
        log.ifDebug("Unregistering from ClusterClient")
        try {
          clusterClient.removeListener(listenerKey)
        } catch {
          case ex: ClusterShutdownException => // oh well, cluster is already shut down
        }
      }

      log.ifDebug("Closing sockets")
      clusterIoClient.shutdown

      log.ifInfo("NetworkClient shut down")
    }
  }
}
