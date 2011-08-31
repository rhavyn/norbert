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

    doSendRequest(PartitionedRequest(request, node, Set(id), (node: Node, ids: Set[PartitionedId]) => request, is, os, callback))
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
   * @param message the request to send
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
    sendRequest(ids, (node: Node, ids: Set[PartitionedId]) => request)(is, os)
  }

  /**
   * Sends a <code>Message</code> to the specified <code>PartitionedId</code>s. The <code>PartitionedNetworkClient</code>
   * will interact with the current <code>PartitionedLoadBalancer</code> to calculate which <code>Node</code>s the message
   * must be sent to.  This method is asynchronous and will return immediately.
   *
   * @param ids the <code>PartitionedId</code>s to which the message is addressed
   * @param message the message to send
   * @param requestBuilder A method which allows the user to generate a specialized request for a set of partitions
   * before it is sent to the <code>Node</code>.
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
    sendRequest(ids, requestBuilder, 0)
  }

  /**
   * Sends a <code>Message</code> to the specified <code>PartitionedId</code>s. The <code>PartitionedNetworkClient</code>
   * will interact with the current <code>PartitionedLoadBalancer</code> to calculate which <code>Node</code>s the message
   * must be sent to.  This method is asynchronous and will return immediately.
   *
   * @param ids the <code>PartitionedId</code>s to which the message is addressed
   * @param message the message to send
   * @param requestBuilder A method which allows the user to generate a specialized request for a set of partitions
   * before it is sent to the <code>Node</code>.
   * @param maxRetry maxium # of retry attempts
   *
   * @return a <code>ResponseIterator</code>. One response will be returned by each <code>Node</code>
   * the message was sent to.
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   */
  // TODO: investigate interplay between default parameter and implicits
  def sendRequest[RequestMsg, ResponseMsg](ids: Set[PartitionedId], requestBuilder: (Node, Set[PartitionedId]) => RequestMsg, maxRetry: Int)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg] = doIfConnected {
    if (ids == null || requestBuilder == null) throw new NullPointerException
    val nodes = calculateNodesFromIds(ids)
    val queue = new ResponseQueue[ResponseMsg]
    val resIter = new NorbertDynamicResponseIterator[ResponseMsg](nodes.size, queue)
    nodes.foreach { case (node, idsForNode) =>
      try {
        doSendRequest(PartitionedRequest(requestBuilder(node, idsForNode), node, idsForNode, requestBuilder, is, os, if (maxRetry == 0) queue.+= else retryCallback[RequestMsg, ResponseMsg](queue.+=, maxRetry), 0, Some(resIter)))
      } catch {
        case ex: Exception => queue += Left(ex)
      }
    }
    resIter
  }

  /**
   * Sends a <code>Message</code> to the specified <code>PartitionedId</code>s. The <code>PartitionedNetworkClient</code>
   * will interact with the current <code>PartitionedLoadBalancer</code> to calculate which <code>Node</code>s the message
   * must be sent to.  This method is synchronous and will return once the responseAggregator has returned a value.
   *
   * @param ids the <code>PartitionedId</code>s to which the message is addressed
   * @param message the message to send
   * @param requestBuilder A method which allows the user to generate a specialized request for a set of partitions
   * before it is sent to the <code>Node</code>.
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
  def sendRequest[RequestMsg, ResponseMsg, Result](ids: Set[PartitionedId],
                                                   requestBuilder: (Node, Set[PartitionedId]) => RequestMsg,
                                                   responseAggregator: (ResponseIterator[ResponseMsg]) => Result)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Result = doIfConnected {
    if (responseAggregator == null) throw new NullPointerException
    responseAggregator(sendRequest[RequestMsg, ResponseMsg](ids, requestBuilder))
  }

  /**
   * Sends a <code>RequestMessage</code> to one replica of the cluster. This is a broadcast intended for read operations on the cluster, like searching every partition for some data.
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
    sendRequestToOneReplica((node: Node, partitions: Set[Int]) => request)(is, os)
  }

  /**
   * Sends a <code>RequestMessage</code> to one replica of the cluster. This is a broadcast intended for read operations on the cluster, like searching every partition for some data.
   *
   * @param requestBuilder A function to generate a request for the chosen node/partitions to send the request to
   *
   * @return a <code>ResponseIterator</code>. One response will be returned by each <code>Node</code>
   * the message was sent to.
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   */
  def sendRequestToOneReplica[RequestMsg, ResponseMsg](requestBuilder: (Node, Set[Int]) => RequestMsg)
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): ResponseIterator[ResponseMsg]  = doIfConnected {
    val nodes = loadBalancer.getOrElse(throw new ClusterDisconnectedException).fold(ex => throw ex,
      lb => lb.nodesForOneReplica)

    if (nodes.isEmpty) throw new NoNodesAvailableException("Unable to satisfy request, no node available for request")

    val queue = new ResponseQueue[ResponseMsg]

    nodes.foreach { case (node, ids) =>
      doSendRequest(PartitionedRequest(requestBuilder(node, ids), node, ids, requestBuilder, is, os, queue.+=))
    }

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

  /**
   * Internal callback wrapper to handle partial failures via RequestAccess
   */
  private[partitioned] def retryCallback[RequestMsg, ResponseMsg](underlying: Either[Throwable, ResponseMsg] => Unit, maxRetry: Int)(res: Either[Throwable, ResponseMsg])
  (implicit is: InputSerializer[RequestMsg, ResponseMsg], os: OutputSerializer[RequestMsg, ResponseMsg]): Unit = {
    def propagate(t: Throwable) { log.info("Propagate exception(%s) to client".format(t)); underlying(Left(t)) }
    def handleFailure(t: Throwable) {
      t match {
        case ra: RequestAccess[PartitionedRequest[PartitionedId, RequestMsg, ResponseMsg]] =>
          log.info("Caught exception(%s) for request %s".format(ra, ra.request))
          val prequest = ra.request
          val requestBuilder = prequest.requestBuilder
          if (prequest.retryAttempt < maxRetry && prequest.responseIterator.isDefined && prequest.responseIterator.get.isInstanceOf[DynamicResponseIterator[ResponseMsg]]) {
            try {
              val nodes = calculateNodesFromIds(prequest.partitionedIds, Set(prequest.node), 3)
              if (nodes.keySet.size > 1) {
                log.debug("Adjust responseIterator size by: %d".format(nodes.keySet.size - 1))
                prequest.responseIterator.get.asInstanceOf[DynamicResponseIterator[ResponseMsg]].addAndGet(nodes.keySet.size - 1)
              }
              nodes.foreach {
                case (node, idsForNode) =>
                  val request1 = PartitionedRequest(requestBuilder(node, idsForNode), node, idsForNode, requestBuilder, is, os, retryCallback[RequestMsg, ResponseMsg](underlying, maxRetry), prequest.retryAttempt + 1, prequest.responseIterator)
                  log.debug("Resend request: %s".format(request1))
                  doSendRequest(request1)
              }
            } catch {
              case t1: Throwable =>
                log.debug("Exception(%s) caught during retry".format(t1))
                propagate(t)
            }
          } else propagate(t)
        case _: Throwable => propagate(t)
      }
    }
    if (underlying == null)
      throw new NullPointerException
    if (maxRetry <= 0)
      res.fold(t => propagate(t), result => underlying(Right(result)))
    else
      res.fold(t => handleFailure(t), result => underlying(Right(result)))
  }

  /**
   * For retry attempts. Failing nodes excluded
   */
  private[partitioned] def calculateNodesFromIds(ids: Set[PartitionedId], excludedNodes: Set[Node], maxTries: Int) = {
    val lb = loadBalancer.getOrElse(throw new ClusterDisconnectedException).fold(ex => throw ex, lb => lb)

    ids.foldLeft(Map[Node, Set[PartitionedId]]().withDefaultValue(Set())) { (map, id) =>
      val optNode: Option[Node] = lb.nextNode(id)
      if (optNode.isEmpty)
        throw new NoNodesAvailableException("Unable to satisfy request, no node available for id %s".format(id))
      var node = optNode.get
      if (excludedNodes.contains(node)) {
        var tries = 0
        do {
          node = lb.nextNode(id).getOrElse(throw new NoNodesAvailableException("Unable to satisfy request, no node available for id %s".format(id)))
          log.info("try#%d: excluded=%s, nextNode(%d)=%s".format(tries, excludedNodes, id, node))
          tries += 1
        } while (excludedNodes.contains(node) && tries <= maxTries)
      }
      map.updated(node, map(node) + id)
    }
  }

}
