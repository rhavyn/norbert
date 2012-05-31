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
import scala.Option;

import java.util.Set;

public class RoundRobinLoadBalancerFactory implements LoadBalancerFactory {
    private final com.linkedin.norbert.network.client.loadbalancer.RoundRobinLoadBalancerFactory scalaLbf =
            new com.linkedin.norbert.network.client.loadbalancer.RoundRobinLoadBalancerFactory();

    @Override
    public LoadBalancer newLoadBalancer(Set<Endpoint> endpoints) throws InvalidClusterException {
      final com.linkedin.norbert.network.client.loadbalancer.LoadBalancer loadBalancer =
        scalaLbf.newLoadBalancer(EndpointConversions.convertJavaEndpointSet(endpoints));

      return new LoadBalancer() {
        @Override
        public Node nextNode() {
          Option<com.linkedin.norbert.cluster.Node> node = loadBalancer.nextNode();
          if(node.isDefined())
            return JavaNode.apply(node.get());
          else
            return null;
        }
      };
    }
}
