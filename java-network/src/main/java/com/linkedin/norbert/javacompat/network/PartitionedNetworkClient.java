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

import java.util.Set;
import java.util.concurrent.Future;

import com.linkedin.norbert.cluster.ClusterDisconnectedException;
import com.linkedin.norbert.cluster.InvalidClusterException;
import com.linkedin.norbert.network.NoNodesAvailableException;
import com.linkedin.norbert.network.ResponseIterator;
import com.linkedin.norbert.network.Serializer;

public interface PartitionedNetworkClient<PartitionedId> extends BaseNetworkClient {
  /**
   * Sends a <code>Message</code> to the specified <code>PartitionedId</code>. The <code>PartitionedNetworkClient</code>
   * will interact with the current <code>PartitionedLoadBalancer</code> to calculate which <code>Node</code> the message
   * must be sent to.  This method is asynchronous and will return immediately.
   *
   * @param id the <code>PartitionedId</code> to which the message is addressed
   * @param request the message to send
   *
   * @return a future which will become available when a response to the message is received
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   */
  <RequestMsg, ResponseMsg> Future<ResponseMsg> sendRequest(PartitionedId id, RequestMsg request, Serializer<RequestMsg, ResponseMsg> serializer) throws InvalidClusterException, NoNodesAvailableException, ClusterDisconnectedException;

  /**
   * Sends a <code>Message</code> to the specified <code>PartitionedId</code>s. The <code>PartitionedNetworkClient</code>
   * will interact with the current <code>PartitionedLoadBalancer</code> to calculate which <code>Node</code>s the message
   * must be sent to.  This method is asynchronous and will return immediately.
   *
   * @param ids the <code>PartitionedId</code>s to which the message is addressed
   * @param request the message to send
   *
   * @return a <code>ResponseIterator</code>. One response will be returned by each <code>Node</code>
   * the message was sent to.
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   */
  <RequestMsg, ResponseMsg> ResponseIterator<ResponseMsg> sendRequest(Set<PartitionedId> ids, RequestMsg request, Serializer<RequestMsg, ResponseMsg> serializer) throws InvalidClusterException, NoNodesAvailableException, ClusterDisconnectedException;

  /**
   * Sends a <code>Message</code> to the specified <code>PartitionedId</code>s. The <code>PartitionedNetworkClient</code>
   * will interact with the current <code>PartitionedLoadBalancer</code> to calculate which <code>Node</code>s the message
   * must be sent to.  This method is synchronous and will return once the <code>ScatterGatherHandler</code>
   * has returned a value.
   *
   * @param ids the <code>PartitionedId</code>s to which the message is addressed
   * @param scatterGather the <code>ScatterGatherHandler</code> that should be used to process the request
   * and responses.
   *
   * @return the return value of the <code>ScatterGatherHandler</code>
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   * @throws Exception any exception thrown by <code>ScatterGatherHandler</code> will be passed through to the client
   */
  <T, RequestMsg, ResponseMsg> T sendRequest(Set<PartitionedId> ids, ScatterGatherHandler<RequestMsg, ResponseMsg, T, PartitionedId> scatterGather, Serializer<RequestMsg, ResponseMsg> serializer) throws Exception;

  /**
   * Sends a <code>Message</code> to one replica of the cluster. The <code>PartitionedNetworkClient</code>
   * will interact with the current <code>PartitionedLoadBalancer</code> to calculate which set of <code>Nodes</code>s
   * the message must be sent to.  This method is asynchronous and will return immediately.
   *
   * @param request the message to send
   *
   * @return a <code>ResponseIterator</code>. One response will be returned by each <code>Node</code>
   * the message was sent to.
   * @throws InvalidClusterException thrown if the cluster is currently in an invalid state
   * @throws NoNodesAvailableException thrown if the <code>PartitionedLoadBalancer</code> was unable to provide a <code>Node</code>
   * to send the request to
   * @throws ClusterDisconnectedException thrown if the <code>PartitionedNetworkClient</code> is not connected to the cluster
   */
  <RequestMsg, ResponseMsg> ResponseIterator<ResponseMsg> sendRequestToOneReplica(RequestMsg request, Serializer<RequestMsg, ResponseMsg> serializer) throws InvalidClusterException, NoNodesAvailableException, ClusterDisconnectedException;

}
