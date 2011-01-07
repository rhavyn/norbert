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
