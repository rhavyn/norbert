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
package com.linkedin.norbert.network.javaapi;

import com.linkedin.norbert.cluster.InvalidClusterException;
import com.linkedin.norbert.cluster.javaapi.Node;

import java.util.*;

public abstract class ConsistentHashPartitionedLoadBalancerFactory<PartitionedId> implements PartitionedLoadBalancerFactory<PartitionedId> {
  public PartitionedLoadBalancer<PartitionedId> newLoadBalancer(Set<Node> nodes) throws InvalidClusterException {
    return new ConsistentHashLoadBalancer(nodes);
  }

  abstract protected int hashPartitionedId(PartitionedId id);

  private class ConsistentHashLoadBalancer implements PartitionedLoadBalancer<PartitionedId> {
    private final Map<Integer, List<Node>> nodeMap = new HashMap<Integer, List<Node>>();
    private final Random random = new Random();

    private ConsistentHashLoadBalancer(Set<Node> nodes) {
      for (Node node : nodes) {
        for (int partitionId : node.getPartitionIds()) {
          List<Node> nodeList = nodeMap.get(partitionId);
          if (nodeList == null) {
            nodeList = new ArrayList<Node>();
            nodeMap.put(partitionId, nodeList);
          }
          nodeList.add(node);
        }
      }
    }

    public Node nextNode(PartitionedId partitionedId) {
      List<Node> nodes = nodeMap.get(hashPartitionedId(partitionedId));
      return nodes.get(random.nextInt(nodes.size()));
    }
  }
}
