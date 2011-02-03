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

import org.apache.log4j.Logger;

import com.linkedin.norbert.javacompat.cluster.Node;

public class RingHashLoadBalancerFactory
{
  private static Logger logger = Logger.getLogger(RingHashLoadBalancerFactory.class);
  private final int _numberOfReplicas;
  private final HashFunction<String> _hashingStrategy;

  public RingHashLoadBalancerFactory(HashFunction<String> hashing, int numRep)
  {
    _numberOfReplicas = numRep;
    _hashingStrategy = hashing;
  }

  RingHashLoadBalancer newLoadBalancer(Set<Node> nodes)
  {
    return new RingHashLoadBalancer(_hashingStrategy, _numberOfReplicas, nodes);
  }
}
