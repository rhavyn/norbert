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

import com.linkedin.norbert.cluster.InvalidNodeException;
import com.linkedin.norbert.cluster.Node;
import com.linkedin.norbert.network.NetworkingException;
import com.linkedin.norbert.network.Serializer;

public interface NetworkServer {
  /**
   * Registers a message handler with the <code>NetworkServer</code>. The <code>NetworkServer</code> will call the
   * provided handler when an incoming request of type <code>requestMessage</code> is received.  If a response is
   * expected then a response message should also be provided.
   *
   * @param handler the function to call when an incoming message of type <code>requestMessage</code> is received
   * @param serializer the serializer used to respond to RequestMsgs pairs
   */
  <RequestMsg, ResponseMsg> void registerHandler(RequestHandler<RequestMsg, ResponseMsg> handler, Serializer<RequestMsg, ResponseMsg> serializer);

  /**
   * Binds the network server instance to the wildcard address and the port of the <code>Node</code> identified
   * by the provided nodeId and automatically marks the <code>Node</code> available in the cluster.  A
   * <code>Node</code>'s url must be specified in the format hostname:port.
   *
   * @param nodeId the id of the <code>Node</code> this server is associated with.
   *
   * @throws InvalidNodeException thrown if no <code>Node</code> with the specified <code>nodeId</code> exists
   * @throws NetworkingException thrown if unable to bind
   */
  void bind(int nodeId) throws InvalidNodeException, NetworkingException;

  /**
   * Binds the network server instance to the wildcard address and the port of the <code>Node</code> identified
   * by the provided nodeId and marks the <code>Node</code> available in the cluster if <code>markAvailable</code> is true.  A
   * <code>Node</code>'s url must be specified in the format hostname:port.
   *
   * @param nodeId the id of the <code>Node</code> this server is associated with.
   * @param markAvailable if true marks the <code>Node</code> identified by <code>nodeId</code> as available after binding to
   * the port
   *
   * @throws InvalidNodeException thrown if no <code>Node</code> with the specified <code>nodeId</code> exists or if the
   * format of the <code>Node</code>'s url isn't hostname:port
   * @throws NetworkingException thrown if unable to bind
   */
  void bind(int nodeId, boolean markAvailable) throws InvalidNodeException, NetworkingException;

  /**
   * Returns the <code>Node</code> associated with this server.
   *
   * @return the <code>Node</code> associated with this server
   */
  Node getMyNode();

  /**
   * Marks the node available in the cluster if the server is bound.
   */
  void markAvailable();

  /**
   * Marks the node unavailable in the cluster if bound.
   */
  void markUnavailable();

  /**
   * Shuts down the network server. This results in unbinding from the port, closing the child sockets, and marking the node unavailable.
   */
  void shutdown();
}
