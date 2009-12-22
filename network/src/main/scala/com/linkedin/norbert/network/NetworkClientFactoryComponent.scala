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
package com.linkedin.norbert.network

import com.linkedin.norbert.cluster._
import com.linkedin.norbert.util.Logging
import com.google.protobuf.Message

/**
 * A component which provides a network client for interacting with nodes in a cluster.
 */
trait NetworkClientFactoryComponent {
  this: ChannelPoolComponent with ClusterComponent with RouterFactoryComponent =>

  val networkClientFactory: NetworkClientFactory

  /**
   * The network client interface for interacting with nodes in a cluster.
   */
  trait NetworkClient {
    /**
     * Sends a <code>Message</code> to the specified <code>Id</code>s. The <code>NetworkClient</code>
     * will interact with the <code>Cluster</code> to calculate which <code>Node</code>s the message
     * must be sent to.
     *
     * @param ids the <code>Id</code>s to which the message is addressed
     * @param message the message to send
     *
     * @return a <code>ResponseIterator</code>. One response will be returned by each <code>Node</code>
     * the message was sent to.
     * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
     */
    def sendMessage(ids: Seq[Id], message: Message): ResponseIterator
    
    /**
     * Sends a <code>Message</code> to the specified <code>Id</code>s. The <code>NetworkClient</code>
     * will interact with the <code>Cluster</code> to calculate which <code>Node</code>s the message
     * must be sent to.
     *
     * @param ids the <code>Id</code>s to which the message is addressed
     * @param message the message to send
     * @param messageCustomizer a callback method which allows the user to customize the <code>Message</code>
     * before it is sent to the <code>Node</code>. The callback will receive the original message passed to <code>sendMessage</code>
     * the <code>Node</code> the request is being sent to and the <code>Id</code>s which reside on that
     * <code>Node</code>. The callback should return a <code>Message</code> which has been customized.
     *
     * @return a <code>ResponseIterator</code>. One response will be returned by each <code>Node</code>
     * the message was sent to.
     * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
     */
    def sendMessage(ids: Seq[Id], message: Message, messageCustomizer: (Message, Node, Seq[Id]) => Message): ResponseIterator

    /**
     * Sends a <code>Message</code> to the specified <code>Id</code>s. The <code>NetworkClient</code>
     * will interact with the <code>Cluster</code> to calculate which <code>Node</code>s the message
     * must be sent to.
     *
     * @param ids the <code>Id</code>s to which the message is addressed
     * @param message the message to send
     * @param responseAggregator a callback method which allows the user to aggregate all the responses
     * and return a single object to the caller.  The callback will receive the original message passed to
     * <code>sendMessage</code> and the <code>ResponseIterator</code> for the request.
     *
     * @return the return value of the <code>responseAggregator</code>
     * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
     */
    def sendMessage[A](ids: Seq[Id], message: Message, responseAggregator: (Message, ResponseIterator) => A): A

    /**
     * Sends a <code>Message</code> to the specified <code>Id</code>s. The <code>NetworkClient</code>
     * will interact with the <code>Cluster</code> to calculate which <code>Node</code>s the message
     * must be sent to.
     *
     * @param ids the <code>Id</code>s to which the message is addressed
     * @param message the message to send
     * @param messageCustomizer a callback method which allows the user to customize the <code>Message</code>
     * before it is sent to the <code>Node</code>. The callback will receive the original message passed to <code>sendMessage</code>
     * the <code>Node</code> the request is being sent to and the <code>Id</code>s which reside on that
     * <code>Node</code>. The callback should return a <code>Message</code> which has been customized.
     * @param responseAggregator a callback method which allows the user to aggregate all the responses
     * and return a single object to the caller.  The callback will receive the original message passed to
     * <code>sendMessage</code> and the <code>ResponseIterator</code> for the request.
     *
     * @return the return value of the <code>responseAggregator</code>
     * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
     */
    def sendMessage[A](ids: Seq[Id], message: Message, messageCustomizer: (Message, Node, Seq[Id]) => Message,
                responseAggregator: (Message, ResponseIterator) => A): A

    /**
     * Sends a <code>Message</code> to the specified <code>Node</code>.
     *
     * @param node the <code>Node</code> to send the message
     * @param message the <code>Message</code> to send
     *
     * @return a <code>ResponseIterator</code>
     * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
     * @throws InvalidNodeException thrown if an attempt is made to send a message to an unavailable <code>Node</code>
     */
    def sendMessageToNode(node: Node, message: Message): ResponseIterator

    /**
     * Queries whether or not a connection to the cluster is established.
     *
     * @return true if connected, false otherwise
     */
    def isConnected: Boolean

    /**
     * Closes the <code>NetworkClient</code> and releases resources held.
     * 
     * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
     */
    def close: Unit
  }

  /**
   * Factory for creating instances of <code>NetworkClient</code>.
   */
  class NetworkClientFactory extends Logging {

    /**
     * Create a new <code>NetworkClient</code>.
     *
     * @return a new <code>NetworkClient</code>
     */
    def newNetworkClient: NetworkClient = {
      cluster.awaitConnectionUninterruptibly

      val client = new NetworkClient with ClusterListener {
        @volatile private var routerOption: Option[Router] = cluster.router
        @volatile private var shutdownSwitch = false
    
        def sendMessage(ids: Seq[Id], message: Message): ResponseIterator = doIfNotShutdown {
          val nodes = calculateNodesFromIds(ids)
          log.ifDebug("Sending message [%s] to nodes: %s", message, nodes)
          val request = Request(message, nodes.size)
          channelPool.sendRequest(nodes.keySet, request)
          request.responseIterator
        }

        def sendMessage(ids: Seq[Id], message: Message, messageCustomizer: (Message, Node, Seq[Id]) => Message): ResponseIterator = doIfNotShutdown {
          val nodes = calculateNodesFromIds(ids)
          val responseIterator = new Request.ResponseIteratorImpl(nodes.size)

          nodes.foreach { case (node, r) =>
            val m = messageCustomizer(message, node, r)
            log.ifDebug("Sending message [%s] to node: %s", message, node)
            val request = Request(message, nodes.size, responseIterator)
            channelPool.sendRequest(Set(node), request)
          }

          responseIterator
        }

        def sendMessage[A](ids: Seq[Id], message: Message, responseAggregator: (Message, ResponseIterator) => A): A = {
          responseAggregator(message, sendMessage(ids, message))
        }
        
        def sendMessage[A](ids: Seq[Id], message: Message, messageCustomizer: (Message, Node, Seq[Id]) => Message,
                responseAggregator: (Message, ResponseIterator) => A): A = {
          responseAggregator(message, sendMessage(ids, message, messageCustomizer))
        }

        def sendMessageToNode(node: Node, message: Message): ResponseIterator = doIfNotShutdown {
          if (!node.available) throw new InvalidNodeException("Unable to send request to an offline node")
          
          log.ifDebug("Sending message [%s] to node: %s", message, node)
          val request = Request(message, 1)
          channelPool.sendRequest(Set(node), request)
          request.responseIterator
        }

        def isConnected = routerOption.isDefined

        def close: Unit = doIfNotShutdown {
          cluster.removeListener(this)
        }
        
        def handleClusterEvent(event: ClusterEvent) = event match {
          case ClusterEvents.Connected(_, r) => routerOption = r
          case ClusterEvents.NodesChanged(_, r) => routerOption = r
          case ClusterEvents.Disconnected => routerOption = None
          case ClusterEvents.Shutdown =>
            shutdownSwitch = true
            routerOption = None
        }

        private def calculateNodesFromIds(ids: Seq[Id]) = {
          val router = routerOption.getOrElse(throw new ClusterDisconnectedException("Cannot send message when the cluster is disconnected"))
          ids.foldLeft(Map[Node, List[Id]]().withDefaultValue(Nil)) { (map, id) =>
            val node = router(id).getOrElse(throw new InvalidClusterException("Unable to satisfy request, no node available for %s".format(id)))
            map(node) = id :: map(node)
          }
        }

        private def doIfNotShutdown[A](block: => A): A = {
          if (!shutdownSwitch) block else throw new ClusterShutdownException
        }
      }

      cluster.addListener(client)
      client
    }

    /**
     * Shuts down the <code>NetworkClientFactory</code>. The results in the closing of all open network
     * sockets and shutting down the underlying <code>Cluster</code> instance.
     */
    def shutdown: Unit = {
      channelPool.shutdown
      cluster.shutdown
    }
  }
}
