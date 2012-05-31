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

import com.linkedin.norbert.cluster.InvalidClusterException;
import com.linkedin.norbert.javacompat.cluster.Node;

/**
 * A factory which can generate <code>LoadBalancer</code>s.
 */
public interface LoadBalancerFactory {
  /**
   * Create a new load balancer instance based on the currently available <code>Node</code>s.
   *
   * @param endpoints the currently available <code>Node</code>s in the cluster
   *
   * @return a new <code>LoadBalancer</code> instance
   * @throws InvalidClusterException thrown to indicate that the current cluster topology is invalid in some way and
   * it is impossible to create a <code>LoadBalancer</code>
   */
  LoadBalancer newLoadBalancer(Set<Endpoint> endpoints) throws InvalidClusterException;
}
