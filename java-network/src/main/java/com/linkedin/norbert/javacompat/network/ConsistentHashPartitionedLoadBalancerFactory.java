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

import com.linkedin.norbert.EndpointConversions;
import com.linkedin.norbert.cluster.InvalidClusterException;
import com.linkedin.norbert.javacompat.cluster.JavaNode;
import com.linkedin.norbert.javacompat.cluster.Node;

import java.util.*;

public abstract class ConsistentHashPartitionedLoadBalancerFactory<PartitionedId> implements PartitionedLoadBalancerFactory<PartitionedId> {
    private final int numPartitions;
    private final boolean serveRequestsIfPartitionUnavailable;
    private final com.linkedin.norbert.network.partitioned.loadbalancer.ConsistentHashPartitionedLoadBalancerFactory<PartitionedId> scalaBalancer;

    public ConsistentHashPartitionedLoadBalancerFactory(int numPartitions, boolean serveRequestsIfPartitionUnavailable) {
      this.numPartitions = numPartitions;
      this.serveRequestsIfPartitionUnavailable = serveRequestsIfPartitionUnavailable;
      scalaBalancer =
        new com.linkedin.norbert.network.partitioned.loadbalancer.ConsistentHashPartitionedLoadBalancerFactory<PartitionedId>(numPartitions, serveRequestsIfPartitionUnavailable) {
          public int calculateHash(PartitionedId id) {
            return hashPartitionedId(id);
          }
        };
    }

    public ConsistentHashPartitionedLoadBalancerFactory(int numPartitions) {
      this(numPartitions, true);
    }


    @Override
    public PartitionedLoadBalancer<PartitionedId> newLoadBalancer(Set<Endpoint> endpoints) throws InvalidClusterException {
      final com.linkedin.norbert.network.partitioned.loadbalancer.PartitionedLoadBalancer<PartitionedId> lbf =
                scalaBalancer.newLoadBalancer(EndpointConversions.convertJavaEndpointSet(endpoints));

      return new PartitionedLoadBalancer<PartitionedId>() {
        public Node nextNode(PartitionedId id) {
          return (Node) lbf.nextNode(id).getOrElse(null);
        }
      };
    }

    abstract protected int hashPartitionedId(PartitionedId id);
}
