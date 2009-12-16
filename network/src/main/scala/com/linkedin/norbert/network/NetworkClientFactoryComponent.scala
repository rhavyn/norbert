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

trait NetworkClientFactoryComponent {
  this: ChannelPoolComponent with ClusterComponent with RouterFactoryComponent =>

  val networkClientFactory: NetworkClientFactory

  trait NetworkClient {
    def sendMessage(ids: Seq[Id], message: Message): ResponseIterator
    def sendMessage(ids: Seq[Id], message: Message, messageCustomizer: (Message, Node, Seq[Id]) => Message): ResponseIterator
    def sendMessage[A](ids: Seq[Id], message: Message, responseAggregator: (Message, ResponseIterator) => A): A
    def sendMessage[A](ids: Seq[Id], message: Message, messageCustomizer: (Message, Node, Seq[Id]) => Message,
                responseAggregator: (Message, ResponseIterator) => A): A

    def sendMessageToNode(node: Node, message: Message): ResponseIterator

    def isConnected: Boolean
    def close: Unit
  }

  class NetworkClientFactory extends Logging {

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

        def sendMessageToNode(node: Node, message: Message): ResponseIterator = {
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

    def shutdown: Unit = {
      channelPool.shutdown
      cluster.shutdown
    }
  }
}
