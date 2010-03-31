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
import com.linkedin.norbert.cluster.Node

abstract class BaseNettyNetworkClient extends BaseNetworkClient {
  val underlying: com.linkedin.nobert.network.common.BaseNetworkClient

  def shutdown = underlying.shutdown

  def broadcastMessage(message: Message) = underlying.broadcaseMessage(message)

  def sendMessageToNode(message: Message, node: Node) = underlying.sendMessageToNode(message, node)

  def registerRequest(requestMessage: Message, responseMessage: Message) = underlying.registerRequest(requestMessage, responseMessage)
}

class NettyNetworkClient(loadBalancerFactory: LoadBalancerFactory) extends BaseNettyNetworkClient with NetworkClient {
  val underlying = com.linkedin.norbert.network.client.NetworkClient(null, new com.linkedin.norbert.network.client.loadbalancer.LoadBalancerFactory {
    def newLoadBalancer(nodes: Seq[Node]) = new com.linkedin.norbert.network.client.loadbalancer.LoadBalancer {
      private val lb = loadBalancerFactory.newLoadBalancer(nodes.toArray)

      def nextNode = lb match {
        case null => None
        case n => Some(n)
      }
    }
  })
  
  underlying.start

  def sendMessage(message: Message) = underlying.sendMessage(message)

  def sendMessageToNode(message: Message, node: Node) = underlying.sendMessageToNode(message, node)
}
