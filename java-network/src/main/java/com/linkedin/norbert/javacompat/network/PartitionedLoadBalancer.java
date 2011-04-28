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
import com.linkedin.norbert.javacompat.cluster.Node;

/**
 * A <code>PartitionedLoadBalancer</code> handles calculating the next <code>Node</code> a message should be routed to
 * based on a PartitionedId.
 */
public interface PartitionedLoadBalancer<PartitionedId> {
  /**
   * Returns the next <code>Node</code> a message should be routed to based on the PartitionId provided.
   *
   * @param id the id to be used to calculate partitioning information.
   *
   * @return the <code>Node</code> to route the next message to
   */
  Node nextNode(PartitionedId id);


  /**
   * Returns a set of nodes represents one replica of the cluster, this is used by the PartitionedNetworkClient to handle
   * broadcast to one replica
   *
   * @return the set of <code>Nodes</code> to broadcast the next message to a replica to
   */
  Set<Node> nodesForOneReplica();

}
