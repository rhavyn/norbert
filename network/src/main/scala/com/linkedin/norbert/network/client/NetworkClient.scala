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
package com.linkedin.norbert.network.client

import com.google.protobuf.Message
import java.util.concurrent.atomic.AtomicBoolean
import com.linkedin.norbert.util.Logging
import java.util.concurrent.Future
import loadbalancer.LoadBalancerFactoryComponent
import com.linkedin.norbert.cluster._
import com.linkedin.norbert.network.common.{MessageRegistryComponent, NorbertFuture, NorbertResponseIterator, ClusterIoClientComponent}
import com.linkedin.norbert.network.{InvalidMessageException, NoNodesAvailableException, NetworkNotStartedException, ResponseIterator}

/**
 * The network client interface for interacting with nodes in a cluster.
 */
trait NetworkClient extends Logging {
  this: ClusterClientComponent with ClusterIoClientComponent with LoadBalancerFactoryComponent with MessageRegistryComponent =>

  private val shutdownSwitch = new AtomicBoolean
  private val startedSwitch = new AtomicBoolean
  private var listenerKey: ClusterListenerKey = _
  @volatile private var connected = false
  @volatile private var currentNodes: Seq[Node] = Nil
  @volatile private var loadBalancer: Option[Either[InvalidClusterException, LoadBalancer]] = None

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
            loadBalancer = None
            currentNodes = Nil

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
   * @param responseMessage an instance of the expected response message
   */
  def registerRequest(requestMessage: Message, responseMessage: Message) {
    messageRegistry.registerMessage(requestMessage, responseMessage)
  }

  /**
   * Sends a message to a node in the cluster. The <code>NetworkClient</code> defers to the current
   * <code>LoadBalancer</code> to decide which <code>Node</code> the message should be sent to.
   *
   * @param message the message to send
   *
   * @returns a future which will become available when a response to the message is received
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>LoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>NetworkClient</code> is not connected to the cluster
   * @throws ClusterShutdownException thrown if the cluster has been shut down
   */
  def sendMessage(message: Message): Future[Message] = doIfConnected {
    if (message == null) throw new NullPointerException
    verifyMessageRegistered(message)

    val node = loadBalancer.getOrElse(throw new ClusterDisconnectedException).fold(ex => throw ex,
      lb => lb.nextNode.getOrElse(throw new NoNodesAvailableException("No node available that can handle the message: %s".format(message))))

    val future = new NorbertFuture
    doSendMessage(node, message, e => future.offerResponse(e))

    future
  }

  /**
   * Sends a message to the specified node in the cluster.
   *
   * @param message the message to send
   * @param node the node to send the message to
   *
   * @returns a future which will become available when a response to the message is received
   * @throws InvalidNodeException thrown if the node specified is not currently available
   * @throws ClusterDisconnectedException thrown if the <code>NetworkClient</code> is not connected to the cluster
   * @throws ClusterShutdownException thrown if the cluster has been shut down
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
   * @returns a <code>ResponseIterator</code> which will provide the responses from the nodes in the cluster
   * as they are received
   * @throws ClusterDisconnectedException thrown if the <code>NetworkClient</code> is not connected to the cluster
   * @throws ClusterShutdownException thrown if the cluster has been shut down
   */
  def broadcastMessage(message: Message): ResponseIterator = doIfConnected {
    if (message == null) throw new NullPointerException
    verifyMessageRegistered(message)

    val nodes = currentNodes
    val it = new NorbertResponseIterator(nodes.length)

    currentNodes.foreach(doSendMessage(_, message, e => it.offerResponse(e)))

    it
  }

  def shutdown: Unit = doShutdown(false)

  protected def doSendMessage(node: Node, message: Message, responseHandler: (Either[Throwable, Message]) => Unit) {
    clusterIoClient.sendMessage(node, message, responseHandler)
  }

  // TODO: If a node goes away, it should be removed from the ClusterIoClient
  private def updateCurrentState(nodes: Seq[Node]) = {
    loadBalancer = generateLoadBalancer(nodes)
    currentNodes = nodes
  }

  private def generateLoadBalancer(nodes: Seq[Node]) = try {
    Some(Right(loadBalancerFactory.newLoadBalancer(nodes)))
  } catch {
    case ex: InvalidClusterException =>
      log.ifInfo(ex, "Unable to create new router instance")
      Some(Left(ex))

    case ex: Exception =>
      val msg = "Exception while creating new router instance"
      log.ifError(ex, msg)
      Some(Left(new InvalidClusterException(msg, ex)))
  }

  private def verifyMessageRegistered(message: Message) {
    if (!messageRegistry.contains(message)) throw new InvalidMessageException("The message provided [%s] is not a registered request message".format(message))
  }

  private def doShutdown(fromCluster: Boolean) {
    if (shutdownSwitch.compareAndSet(false, true) && startedSwitch.get) {
      log.ifInfo("Shutting down NetworkClient")

      if (!fromCluster) {
        log.ifDebug("Unregistering from ClusterClient")
        clusterClient.removeListener(listenerKey)
      }

      log.ifDebug("Closing sockets")
      clusterIoClient.shutdown

      log.ifInfo("NetworkClient shut down")
    }
  }

  private def doIfConnected[T](block: => T): T = {
    if (shutdownSwitch.get) throw new ClusterShutdownException
    else if (!startedSwitch.get) throw new NetworkNotStartedException
    else if (!connected) throw new ClusterDisconnectedException
    else block
  }
}
