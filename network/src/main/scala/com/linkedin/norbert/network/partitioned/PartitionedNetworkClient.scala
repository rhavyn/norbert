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
package com.linkedin.norbert.network.partitioned

import com.google.protobuf.Message
import com.linkedin.norbert.network.ResponseIterator
import com.linkedin.norbert.cluster.{ClusterClientComponent, Node}
import java.util.concurrent.Future
import loadbalancer.{PartitionedLoadBalancerFactory, PartitionedLoadBalancerFactoryComponent}
import com.linkedin.norbert.network.client.NetworkClientConfig
import com.linkedin.norbert.network.common.{BaseNetworkClient, ClusterIoClientComponent, MessageRegistryComponent}

object PartitionedNetworkClient {
  def apply[PartitionedId](config: NetworkClientConfig, loadBalancerFactory: PartitionedLoadBalancerFactory[PartitionedId]): PartitionedNetworkClient[PartitionedId] = {
    null
  }
}

/**
 * The network client interface for interacting with nodes in a partitioned cluster.
 */
trait PartitionedNetworkClient[PartitionedId] extends BaseNetworkClient {
  this: ClusterClientComponent with ClusterIoClientComponent with MessageRegistryComponent with PartitionedLoadBalancerFactoryComponent[PartitionedId] =>

  /**
    * Sends a <code>Message</code> to the specified <code>Id</code>. The <code>PartitionedNetworkClient</code>
    * will interact with the <code>Cluster</code> to calculate which <code>Node</code> the message
    * must be sent to.  This method is asynchronous and will return immediately.
    *
    * @param id the <code>Id</code> to which the message is addressed
    * @param message the message to send
    *
   * @returns a future which will become available when a response to the message is received
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   * @throws ClusterShutdownException thrown if the cluster has been shut down
    */
  def sendMessage(id: PartitionedId, message: Message): Future[Message]
  
 /**
   * Sends a <code>Message</code> to the specified <code>Id</code>s. The <code>PartitionedNetworkClient</code>
   * will interact with the <code>Cluster</code> to calculate which <code>Node</code>s the message
   * must be sent to.  This method is asynchronous and will return immediately.
   *
   * @param ids the <code>Id</code>s to which the message is addressed
   * @param message the message to send
   *
   * @return a <code>ResponseIterator</code>. One response will be returned by each <code>Node</code>
   * the message was sent to.
  * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
  * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
  * to send the request to
  * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
  * @throws ClusterShutdownException thrown if the cluster has been shut down
   */
  def sendMessage(ids: Seq[PartitionedId], message: Message): ResponseIterator

  /**
   * Sends a <code>Message</code> to the specified <code>Id</code>s. The <code>PartitionedNetworkClient</code>
   * will interact with the <code>Cluster</code> to calculate which <code>Node</code>s the message
   * must be sent to.  This method is asynchronous and will return immediately.
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
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   * @throws ClusterShutdownException thrown if the cluster has been shut down
   */
  def sendMessage(ids: Seq[PartitionedId], message: Message, messageCustomizer: (Message, Node, Seq[PartitionedId]) => Message): ResponseIterator

  /**
   * Sends a <code>Message</code> to the specified <code>Id</code>s. The <code>PartitionedNetworkClient</code>
   * will interact with the <code>Cluster</code> to calculate which <code>Node</code>s the message
   * must be sent to.  This method is synchronous and will return once the responseAggregator has returned a value.
   *
   * @param ids the <code>Id</code>s to which the message is addressed
   * @param message the message to send
   * @param responseAggregator a callback method which allows the user to aggregate all the responses
   * and return a single object to the caller.  The callback will receive the original message passed to
   * <code>sendMessage</code> and the <code>ResponseIterator</code> for the request.
   *
   * @return the return value of the <code>responseAggregator</code>
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   * @throws ClusterShutdownException thrown if the cluster has been shut down
   * @throws Exception any exception thrown by <code>responseAggregator</code> will be passed through to the client
   */
  def sendMessage[A](ids: Seq[PartitionedId], message: Message, responseAggregator: (Message, ResponseIterator) => A): A

  /**
   * Sends a <code>Message</code> to the specified <code>Id</code>s. The <code>PartitionedNetworkClient</code>
   * will interact with the <code>Cluster</code> to calculate which <code>Node</code>s the message
   * must be sent to.  This method is synchronous and will return once the responseAggregator has returned a value.
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
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   * @throws ClusterShutdownException thrown if the cluster has been shut down
   * @throws Exception any exception thrown by <code>responseAggregator</code> will be passed through to the client
   */
  def sendMessage[A](ids: Seq[PartitionedId], message: Message, messageCustomizer: (Message, Node, Seq[PartitionedId]) => Message,
              responseAggregator: (Message, ResponseIterator) => A): A
}
