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
package com.linkedin.norbert
package network
package common

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.Future
import cluster._
import logging.Logging
import jmx.JMX

trait BaseNetworkClient extends Logging {
  this: ClusterClientComponent with ClusterIoClientComponent =>

  @volatile protected var currentNodes: Set[Node] = Set()
  @volatile protected var endpoints: Set[Endpoint] = Set()

  @volatile protected var currentlyConnected = false
  @volatile protected var previouslyConnected = false

  protected val startedSwitch = new AtomicBoolean
  protected val shutdownSwitch = new AtomicBoolean

  private var listenerKey: ClusterListenerKey = _

  def start {
    if (startedSwitch.compareAndSet(false, true)) {
      log.debug("Ensuring cluster is started")
      clusterClient.start
      clusterClient.awaitConnectionUninterruptibly
      updateCurrentState(clusterClient.nodes)
      currentlyConnected = clusterClient.isConnected
      previouslyConnected = currentlyConnected

      listenerKey = clusterClient.addListener(new ClusterListener {
        def handleClusterEvent(event: ClusterEvent) = event match {
          case ClusterEvents.Connected(nodes) =>
            updateCurrentState(nodes)
            previouslyConnected = true
            currentlyConnected = true

          case ClusterEvents.NodesChanged(nodes) => updateCurrentState(nodes)

          case ClusterEvents.Disconnected =>
            currentlyConnected = false
            log.warn("Disconnected from the cluster. We will continue to operate with the previous set of nodes. %s".format(currentNodes))

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
//  def registerRequest(requestMessage: Message, responseMessage: Message) {
//    messageRegistry.registerMessage(requestMessage, responseMessage)
//  }

  /**
   * Sends a request to the specified node in the cluster.
   *
   * @param request the message to send
   * @param node the node to send the message to
   * @param callback a method to be called with either a Throwable in the case of an error along
   * the way or a ResponseMsg representing the result
   *
   * @throws InvalidNodeException thrown if the node specified is not currently available
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   */
  def sendRequestToNode[RequestMsg, ResponseMsg](request: RequestMsg, node: Node, callback: Either[Throwable, ResponseMsg] => Unit)
   (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Unit = doIfConnected {
    if (request == null || node == null) throw new NullPointerException

    val candidate = currentNodes.filter(_ == node)
    if (candidate.size == 0) throw new InvalidNodeException("Unable to send message, %s is not available".format(node))

    doSendRequest(node, request, callback)
  }


  /**
   * Sends a request to the specified node in the cluster.
   *
   * @param request the message to send
   * @param node the node to send the message to
   *
   * @return a future which will become available when a response to the message is received
   * @throws InvalidNodeException thrown if the node specified is not currently available
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   */
  def sendRequestToNode[RequestMsg, ResponseMsg](request: RequestMsg, node: Node)
   (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Future[ResponseMsg] = {
    val future = new FutureAdapter[ResponseMsg]
    sendRequestToNode(request, node, future)
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
  def broadcastMessage[RequestMsg, ResponseMsg](request: RequestMsg)
   (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] = doIfConnected {
    if (request == null) throw new NullPointerException

    val nodes = currentNodes
    val queue = new ResponseQueue[ResponseMsg]

    currentNodes.foreach(doSendRequest(_, request, queue +=))

    new NorbertResponseIterator(nodes.size, queue)
  }

  /**
   * Shuts down the <code>NetworkClient</code> and releases resources held.
   */
  def shutdown: Unit = doShutdown(false)

  protected def updateLoadBalancer(nodes: Set[Endpoint]): Unit

  protected def doSendRequest[RequestMsg, ResponseMsg](node: Node, request: RequestMsg, callback: Either[Throwable, ResponseMsg] => Unit)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Unit = {
    clusterIoClient.sendMessage(node, Request(request, node, is, os, callback))
  }

  protected def doIfConnected[T](block: => T): T = {
    if (shutdownSwitch.get) throw new NetworkShutdownException
    else if (!startedSwitch.get) throw new NetworkNotStartedException
    else if (!previouslyConnected) throw new ClusterDisconnectedException
    else block
  }


  private def updateCurrentState(nodes: Set[Node]) {
    currentNodes = nodes
    endpoints = clusterIoClient.nodesChanged(nodes)
    updateLoadBalancer(endpoints)
  }

  private def doShutdown(fromCluster: Boolean) {
    if (shutdownSwitch.compareAndSet(false, true) && startedSwitch.get) {
      log.info("Shutting down NetworkClient")

      if (!fromCluster) {
        log.debug("Unregistering from ClusterClient")
        try {
          clusterClient.removeListener(listenerKey)
        } catch {
          case ex: ClusterShutdownException => // oh well, cluster is already shut down
        }
      }

      log.debug("Closing sockets")
      clusterIoClient.shutdown

      log.info("NetworkClient shut down")
    }
  }
}
