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
package com.linkedin.norbert.javacompat.network;

import java.util.concurrent.Future;

import com.linkedin.norbert.cluster.ClusterDisconnectedException;
import com.linkedin.norbert.cluster.InvalidNodeException;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.network.ResponseIterator;
import com.linkedin.norbert.network.Serializer;

public interface BaseNetworkClient {
  /**
   * Registers a request/response message pair with the <code>NetworkClient</code>.  Requests and their associated
   * responses must be registered or an <code>InvalidMessageException</code> will be thrown when an attempt to send
   * a <code>Message</code> is made.
   *
   * @param requestMessage an instance of an outgoing request message
   * @param responseMessage an instance of the expected response message or null if this is a one way message
   */
//  void registerRequest(RequestMsg requestMessage, Message responseMessage);

  /**
   * Sends a message to the specified node in the cluster.
   *
   * @param request the message to send
   * @param node the node to send the message to
   * @param serializer the serializer needed to encode and decode the request and response pairs to byte arrays
   *
   * @return a future which will become available when a response to the message is received
   * @throws InvalidNodeException thrown if the node specified is not currently available
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   */
  <RequestMsg, ResponseMsg> Future<ResponseMsg> sendRequestToNode(RequestMsg request, Node node, Serializer<RequestMsg, ResponseMsg> serializer) throws InvalidNodeException, ClusterDisconnectedException;

  /**
   * Broadcasts a message to all the currently available nodes in the cluster.
   *
   * @param request the message to send
   * @param serializer the serializer needed to encode and decode the request and response pairs to byte arrays
   *
   * @return a <code>ResponseIterator</code> which will provide the responses from the nodes in the cluster
   * as they are received
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   */
  <RequestMsg, ResponseMsg> ResponseIterator<ResponseMsg> broadcastMessage(RequestMsg request, Serializer<RequestMsg, ResponseMsg> serializer) throws ClusterDisconnectedException;

  /**
   * Shuts down the <code>NetworkClient</code> and releases resources held.
   */
  void shutdown();
}
