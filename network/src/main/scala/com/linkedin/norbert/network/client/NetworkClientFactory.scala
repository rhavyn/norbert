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
import com.linkedin.norbert.network.{NoNodesAvailableException, NetworkNotStartedException, ResponseIterator}
import com.linkedin.norbert.network.common.{NorbertFuture, NorbertResponseIterator, ClusterIoClientComponent}

/**
 * The network client interface for interacting with nodes in a cluster.
 */
trait NetworkClient {
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
  def sendMessage(message: Message): Future[Message]

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
  def sendMessageToNode(message: Message, node: Node): Future[Message]

  /**
   * Broadcasts a message to all the currently available nodes in the cluster.
   *
   * @returns a <code>ResponseIterator</code> which will provide the responses from the nodes in the cluster
   * as they are received
   * @throws ClusterDisconnectedException thrown if the <code>NetworkClient</code> is not connected to the cluster
   * @throws ClusterShutdownException thrown if the cluster has been shut down
   */
  def broadcastMessage(message: Message): ResponseIterator
}

trait NetworkClientFactory extends Logging {
  this: ClusterClientComponent with ClusterIoClientComponent with LoadBalancerFactoryComponent =>

  private val startedSwitch = new AtomicBoolean
  private val shutdownSwitch = new AtomicBoolean
  @volatile private var connected = false
  @volatile private var currentNodes: Seq[Node] = Nil
  @volatile private var loadBalancer: Option[Either[InvalidClusterException, LoadBalancer]] = None

  def start: Unit = {
    if (shutdownSwitch.get) throw new ClusterShutdownException

    if (startedSwitch.compareAndSet(false, true)) {
      log.ifDebug("Starting cluster...")
      clusterClient.start
      updateCurrentState(clusterClient.nodes)
      connected = clusterClient.isConnected

      clusterClient.addListener(new ClusterListener {
        def handleClusterEvent(event: ClusterEvent) = event match {
          case ClusterEvents.Connected(nodes) =>
            updateCurrentState(nodes)
            connected = true

          case ClusterEvents.NodesChanged(nodes) => updateCurrentState(nodes)

          case ClusterEvents.Disconnected =>
            connected = false
            loadBalancer = None
            currentNodes = Nil

          case ClusterEvents.Shutdown => shutdownSwitch.set(true)
        }
      })
    }
  }

  def newNetworkClient: NetworkClient = {
    if (shutdownSwitch.get) throw new ClusterShutdownException
    if (!startedSwitch.get) throw new NetworkNotStartedException

    new NetworkClient {
      def broadcastMessage(message: Message) = doIfConnected {
        if (message == null) throw new NullPointerException
        val nodes = currentNodes
        val it = new NorbertResponseIterator(nodes.length)

        currentNodes.foreach(clusterIoClient.sendMessage(_, message, e => it.offerResponse(e)))

        it
      }

      def sendMessageToNode(message: Message, node: Node) = doIfConnected {
        if (message == null || node == null) throw new NullPointerException

        val candidate = currentNodes.filter(_ == node)
        if (candidate.length == 0) throw new InvalidNodeException("Unable to send message, %s is not available".format(node))

        val future = new NorbertFuture
        clusterIoClient.sendMessage(node, message, e => future.offerResponse(e))

        future
      }

      def sendMessage(message: Message) = doIfConnected {
        if (message == null) throw new NullPointerException

        val node = loadBalancer match {
          case Some(Left(ex)) => throw ex
          case Some(Right(lb)) => lb.nextNode.getOrElse(throw new NoNodesAvailableException("No node available that can handle the message: %s".format(message)))
          case None => throw new ClusterDisconnectedException
        }

        val future = new NorbertFuture
        clusterIoClient.sendMessage(node, message, e => future.offerResponse(e))

        future
      }

      private def doIfConnected[T](block: => T): T =
        if (shutdownSwitch.get) throw new ClusterShutdownException
        else if (!connected) throw new ClusterDisconnectedException
        else block
    }
  }

  def shutdown: Unit = if (shutdownSwitch.compareAndSet(false, true)) {
    log.ifDebug("Shutting down NetworkClientFactory...")

    log.ifDebug("Shutting down cluster...")
    clusterClient.shutdown

    log.ifDebug("Shutting down ClusterIoClient...")
    clusterIoClient.shutdown

    log.ifDebug("NetworkClientFactory shut down")
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
}
