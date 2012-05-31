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
import com.linkedin.norbert.cluster.InvalidClusterException;
import com.linkedin.norbert.network.NoNodesAvailableException;
import com.linkedin.norbert.network.Serializer;

public interface NetworkClient extends BaseNetworkClient {
  /**
   * Sends a request to a node in the cluster. The <code>NetworkClient</code> defers to the current
   * <code>LoadBalancer</code> to decide which <code>Node</code> the request should be sent to.
   *
   * @param request the request to send
   * @param serializer the serializer needed to encode and decode the request and response pairs to byte arrays
   *
   * @return a future which will become available when a response to the request is received
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>LoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the cluster is not connected when the method is called
   */
  <RequestMsg, ResponseMsg> Future<ResponseMsg> sendRequest(RequestMsg request, Serializer<RequestMsg, ResponseMsg> serializer) throws InvalidClusterException, NoNodesAvailableException, ClusterDisconnectedException;
}
