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

import org.specs.Specification
import org.specs.mock.Mockito
import client.NetworkClient
import client.loadbalancer.{LoadBalancerFactory, LoadBalancer, LoadBalancerFactoryComponent}
import server.{MessageExecutorComponent, MessageExecutor}
import cluster.{Node, ClusterClientComponent, ClusterClient}
import com.google.protobuf.Message

class LocalMessageExecutionSpec extends Specification with Mockito with SampleMessage {
  val clusterClient = mock[ClusterClient]

  val messageExecutor = new MessageExecutor {
    var called = false
    var request: Any = _

    def shutdown = {}

    def executeMessage[RequestMsg, ResponseMsg](request: RequestMsg, responseHandler: (Either[Exception, ResponseMsg]) => Unit)(implicit serializer: Serializer[RequestMsg, ResponseMsg]) = {
      called = true
      this.request = request

      val response = null.asInstanceOf[ResponseMsg]

      responseHandler(Right(response))
    }
  }

  val networkClient = new NetworkClient with ClusterClientComponent with ClusterIoClientComponent with LoadBalancerFactoryComponent
      with MessageExecutorComponent with LocalMessageExecution {
    val lb = mock[LoadBalancer]
    val loadBalancerFactory = mock[LoadBalancerFactory]
    val clusterIoClient = mock[ClusterIoClient]
//    val messageRegistry = mock[MessageRegistry]
    val clusterClient = LocalMessageExecutionSpec.this.clusterClient
    val messageExecutor = LocalMessageExecutionSpec.this.messageExecutor
    val myNode = Node(1, "localhost:31313", true)
  }


  val nodes = Set(Node(1, "", true), Node(2, "", true), Node(3, "", true))
  val endpoints = nodes.map { n => new Endpoint {
    def node = n
    def canServeRequests = true
  }}
  val message = mock[Message]

//  networkClient.messageRegistry.contains(any[Message]) returns true
  clusterClient.nodes returns nodes
  clusterClient.isConnected returns true
  networkClient.clusterIoClient.nodesChanged(nodes) returns endpoints
  networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.lb

  "LocalMessageExecution" should {
    "call the MessageExecutor if myNode is equal to the node the request is to be sent to" in {
      networkClient.lb.nextNode returns Some(networkClient.myNode)

      networkClient.start

      networkClient.sendRequest(request) must notBeNull

      messageExecutor.called must beTrue
      messageExecutor.request must be_==(request)
    }

    "not call the MessageExecutor if myNode is not equal to the node the request is to be sent to" in {
      networkClient.lb.nextNode returns Some(Node(2, "", true))

      networkClient.start
      networkClient.sendRequest(request) must notBeNull

      messageExecutor.called must beFalse
    }
  }
}
