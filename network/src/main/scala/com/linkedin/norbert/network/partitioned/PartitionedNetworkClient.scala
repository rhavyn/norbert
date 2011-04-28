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
package partitioned

import java.util.concurrent.Future
import common._
import loadbalancer.{PartitionedLoadBalancer, PartitionedLoadBalancerFactoryComponent, PartitionedLoadBalancerFactory}
import server.{MessageExecutorComponent, NetworkServer}
import netty.NettyPartitionedNetworkClient
import client.NetworkClientConfig
import cluster.{Node, ClusterDisconnectedException, InvalidClusterException, ClusterClientComponent}

object PartitionedNetworkClient {
  def apply[PartitionedId](config: NetworkClientConfig, loadBalancerFactory: PartitionedLoadBalancerFactory[PartitionedId]): PartitionedNetworkClient[PartitionedId] = {
    val nc = new NettyPartitionedNetworkClient(config, loadBalancerFactory)
    nc.start
    nc
  }

  def apply[PartitionedId](config: NetworkClientConfig, loadBalancerFactory: PartitionedLoadBalancerFactory[PartitionedId],
      server: NetworkServer): PartitionedNetworkClient[PartitionedId] = {
    val nc = new NettyPartitionedNetworkClient(config, loadBalancerFactory) with LocalMessageExecution with MessageExecutorComponent {
      val messageExecutor = server.asInstanceOf[MessageExecutorComponent].messageExecutor
      val myNode = server.myNode
    }
    nc.start
    nc
  }

}

/**
 * The network client interface for interacting with nodes in a partitioned cluster.
 */
trait PartitionedNetworkClient[PartitionedId] extends BaseNetworkClient {
  this: ClusterClientComponent with ClusterIoClientComponent  with PartitionedLoadBalancerFactoryComponent[PartitionedId] =>

  @volatile private var loadBalancer: Option[Either[InvalidClusterException, PartitionedLoadBalancer[PartitionedId]]] = None

  def sendRequest[RequestMsg, ResponseMsg](id: PartitionedId, request: RequestMsg, callback: Either[Throwable, ResponseMsg] => Unit)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Unit = doIfConnected {
    if (id == null || request == null) throw new NullPointerException

    val node = loadBalancer.getOrElse(throw new ClusterDisconnectedException).fold(ex => throw ex,
      lb => lb.nextNode(id).getOrElse(throw new NoNodesAvailableException("Unable to satisfy request, no node available for id %s".format(id))))

    doSendRequest(node, request, callback)
  }


  /**
   * Sends a <code>Message</code> to the specified <code>PartitionedId</code>. The <code>PartitionedNetworkClient</code>
   * will interact with the current <code>PartitionedLoadBalancer</code> to calculate which <code>Node</code> the message
   * must be sent to.  This method is asynchronous and will return immediately.
   *
   * @param id the <code>PartitionedId</code> to which the message is addressed
   * @param message the message to send
   *
   * @return a future which will become available when a response to the message is received
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   */
  def sendRequest[RequestMsg, ResponseMsg](id: PartitionedId, request: RequestMsg)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Future[ResponseMsg] = {
    val future = new FutureAdapter[ResponseMsg]
    sendRequest(id, request, future)
    future
  }

 /**
  * Sends a <code>Message</code> to the specified <code>PartitionedId</code>s. The <code>PartitionedNetworkClient</code>
  * will interact with the current <code>PartitionedLoadBalancer</code> to calculate which <code>Node</code>s the message
  * must be sent to.  This method is asynchronous and will return immediately.
  *
  * @param ids the <code>PartitionedId</code>s to which the message is addressed
  * @param message the message to send
  *
  * @return a <code>ResponseIterator</code>. One response will be returned by each <code>Node</code>
  * the message was sent to.
  * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
  * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
  * to send the request to
  * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
  */
  def sendRequest[RequestMsg, ResponseMsg](ids: Set[PartitionedId], request: RequestMsg)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] = doIfConnected {
    if (ids == null || request == null) throw new NullPointerException

    val nodes = calculateNodesFromIds(ids)
    val queue = new ResponseQueue[ResponseMsg]
    nodes.keySet.foreach { node => doSendRequest(node, request, queue.+=) }

    new NorbertResponseIterator(nodes.size, queue)
  }

  /**
   * Sends a <code>Message</code> to the specified <code>PartitionedId</code>s. The <code>PartitionedNetworkClient</code>
   * will interact with the current <code>PartitionedLoadBalancer</code> to calculate which <code>Node</code>s the message
   * must be sent to.  This method is asynchronous and will return immediately.
   *
   * @param ids the <code>PartitionedId</code>s to which the message is addressed
   * @param message the message to send
   * @param messageCustomizer a callback method which allows the user to customize the <code>Message</code>
   * before it is sent to the <code>Node</code>. The callback will receive the original message passed to <code>sendRequest</code>
   * the <code>Node</code> the request is being sent to and the <code>Id</code>s which reside on that
   * <code>Node</code>. The callback should return a <code>Message</code> which has been customized.
   *
   * @return a <code>ResponseIterator</code>. One response will be returned by each <code>Node</code>
   * the message was sent to.
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   */
  def sendRequest[RequestMsg, ResponseMsg](ids: Set[PartitionedId], requestBuilder: (Node, Set[PartitionedId]) => RequestMsg)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] = doIfConnected {
    if (ids == null || requestBuilder == null) throw new NullPointerException

    val nodes = calculateNodesFromIds(ids)
    val queue = new ResponseQueue[ResponseMsg]
    nodes.foreach { case (node, idsForNode) =>
      try {
        doSendRequest(node, requestBuilder(node, idsForNode), queue.+=)
      } catch {
        case ex: Exception => queue += Left(ex)
      }
    }

    new NorbertResponseIterator(nodes.size, queue)
  }

  /**
   * Sends a <code>Message</code> to the specified <code>PartitionedId</code>s. The <code>PartitionedNetworkClient</code>
   * will interact with the current <code>PartitionedLoadBalancer</code> to calculate which <code>Node</code>s the message
   * must be sent to.  This method is synchronous and will return once the responseAggregator has returned a value.
   *
   * @param ids the <code>PartitionedId</code>s to which the message is addressed
   * @param message the message to send
   * @param responseAggregator a callback method which allows the user to aggregate all the responses
   * and return a single object to the caller.  The callback will receive the original message passed to
   * <code>sendRequest</code> and the <code>ResponseIterator</code> for the request.
   *
   * @return the return value of the <code>responseAggregator</code>
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   * @throws Exception any exception thrown by <code>responseAggregator</code> will be passed through to the client
   */
  def sendRequest[RequestMsg, ResponseMsg, Result](ids: Set[PartitionedId], request: RequestMsg, responseAggregator: (RequestMsg, ResponseIterator[ResponseMsg]) => Result)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Result = doIfConnected{
    if (responseAggregator == null) throw new NullPointerException
    responseAggregator(request, sendRequest(ids, request))
  }

  /**
   * Sends a <code>Message</code> to the specified <code>PartitionedId</code>s. The <code>PartitionedNetworkClient</code>
   * will interact with the current <code>PartitionedLoadBalancer</code> to calculate which <code>Node</code>s the message
   * must be sent to.  This method is synchronous and will return once the responseAggregator has returned a value.
   *
   * @param ids the <code>PartitionedId</code>s to which the message is addressed
   * @param message the message to send
   * @param messageCustomizer a callback method which allows the user to customize the <code>Message</code>
   * before it is sent to the <code>Node</code>. The callback will receive the original message passed to <code>sendRequest</code>
   * the <code>Node</code> the request is being sent to and the <code>Id</code>s which reside on that
   * <code>Node</code>. The callback should return a <code>Message</code> which has been customized.
   * @param responseAggregator a callback method which allows the user to aggregate all the responses
   * and return a single object to the caller.  The callback will receive the original message passed to
   * <code>sendRequest</code> and the <code>ResponseIterator</code> for the request.
   *
   * @return the return value of the <code>responseAggregator</code>
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   * @throws Exception any exception thrown by <code>responseAggregator</code> will be passed through to the client
   */
  def sendRequest[RequestMsg, ResponseMsg, Result](ids: Set[PartitionedId], requestBuilder: (Node, Set[PartitionedId]) => RequestMsg,
              responseAggregator: (ResponseIterator[ResponseMsg]) => Result)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Result = doIfConnected {
    if (responseAggregator == null) throw new NullPointerException
    responseAggregator(sendRequest(ids, requestBuilder))
  }

  /**
   * Sends a <code>Message</code> to one replica of the cluster. This is a broadcast intended for read operations on the cluster.
   *
   * @param request the request message to be sent
   *
   * @return a <code>ResponseIterator</code>. One response will be returned by each <code>Node</code>
   * the message was sent to.
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   */
  def sendRequestToOneReplica[RequestMsg, ResponseMsg](request: RequestMsg)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg]  = doIfConnected {
    if (request == null) throw new NullPointerException

    val nodes = loadBalancer.getOrElse(throw new ClusterDisconnectedException).fold(ex => throw ex,
      lb => lb.nodesForOneReplica)

    if (nodes.isEmpty) throw new NoNodesAvailableException("Unable to satisfy request, no node available for request")

    val queue = new ResponseQueue[ResponseMsg]
    nodes.foreach { node => doSendRequest(node, request, queue.+=) }

    new NorbertResponseIterator(nodes.size, queue)
  }

  protected def updateLoadBalancer(endpoints: Set[Endpoint]) {
    loadBalancer = if (endpoints != null && endpoints.size > 0) {
      try {
        Some(Right(loadBalancerFactory.newLoadBalancer(endpoints)))
      } catch {
        case ex: InvalidClusterException =>
          log.info(ex, "Unable to create new router instance")
          Some(Left(ex))

        case ex: Exception =>
          val msg = "Exception while creating new router instance"
          log.error(ex, msg)
          Some(Left(new InvalidClusterException(msg, ex)))
      }
    } else {
      None
    }
  }

  private def calculateNodesFromIds(ids: Set[PartitionedId]) = {
    val lb = loadBalancer.getOrElse(throw new ClusterDisconnectedException).fold(ex => throw ex, lb => lb)

    ids.foldLeft(Map[Node, Set[PartitionedId]]().withDefaultValue(Set())) { (map, id) =>
      val node = lb.nextNode(id).getOrElse(throw new NoNodesAvailableException("Unable to satisfy request, no node available for id %s".format(id)))
      map.updated(node, map(node) + id)
    }
  }
}
