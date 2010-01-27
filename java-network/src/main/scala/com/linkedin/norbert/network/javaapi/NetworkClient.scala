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
import com.linkedin.norbert.cluster.{InvalidNodeException, ClusterShutdownException, Node}

/**
 * The network client interface for interacting with nodes in a cluster.
 */
trait NetworkClient {
  /**
   * Sends a <code>Message</code> to the specified ids. The <code>NetworkClient</code>
   * will interact with the <code>Cluster</code> to calculate which <code>Node</code>s the message
   * must be sent to.  This method is asynchronous and will return immediately.
   *
   * @param ids the ids to which the message is addressed
   * @param message the message to send
   *
   * @return a <code>ResponseIterator</code>. One response will be returned by each <code>Node</code>
   * the message was sent to.
   * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
   */
  @throws(classOf[ClusterShutdownException])
  def sendMessage(ids: Array[Int], message: Message): ResponseIterator

  /**
   * Sends a <code>Message</code> to the specified <code>Id</code>s. The <code>NetworkClient</code>
   * will interact with the <code>Cluster</code> to calculate which <code>Node</code>s the message
   * must be sent to.  This method is synchronous and will return once the <code>ScatterGatherHandler</code>
   * has returned a value.
   *
   * @param ids the ids to which the message is addressed
   * @param message the message to send
   * @param scatterGather the <code>ScatterGatherHandler</code> that should be used to process the request
   * and responses.
   *
   * @return the return value of the <code>ScatterGatherHandler</code>
   * @throws ClusterShutdownException thrown if the cluster is shutdown when the method is called
   * @throws Exception any exception thrown by <code>ScatterGatherHandler</code> will be passed through to the client
   */
  @throws(classOf[ClusterShutdownException])
  @throws(classOf[Exception])
  def sendMessage[A](ids: Array[Int], message: Message, scatterGather: ScatterGatherHandler[A]): A

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
  @throws(classOf[ClusterShutdownException])
  @throws(classOf[InvalidNodeException])
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
  @throws(classOf[ClusterShutdownException])
  def close: Unit
}
