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
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.linkedin.norbert.cluster.InvalidClusterException;
import com.linkedin.norbert.javacompat.cluster.Node;

/**
 * Consistent hash load balancer factory.
 * 
 * Consistent hashing is implemented based on http://www.lexemetech.com/2007/11/consistent-hashing.html
 * 
 * @author "Rui Wang<rwang@linkedin.com>" 
 *
 */
public class RingHashPartitionedLoadBalancerFactory implements PartitionedLoadBalancerFactory<Integer>
{
  public static final Logger log = Logger.getLogger(RingHashPartitionedLoadBalancerFactory.class);
  
  // number of Replicas is used to help to produce balanced buckets.
  private final int _numberOfReplicas;
  private final HashFunction<String> _hashingStrategy;
  
  public RingHashPartitionedLoadBalancerFactory(int numberOfReplicas, HashFunction<String> hashingStrategy)
  {
    _numberOfReplicas = numberOfReplicas;
    _hashingStrategy = hashingStrategy;
  }


  public RingHashPartitionedLoadBalancerFactory(int numberOfReplicas){
      this(numberOfReplicas,new HashFunction.MD5HashFunction());
  }
  public PartitionedLoadBalancer<Integer> newLoadBalancer(Set<Endpoint> endpoints) throws InvalidClusterException
  {
    return new RingHashPartitionedLoadBalancer(endpoints);
  }

  private class RingHashPartitionedLoadBalancer implements PartitionedLoadBalancer<Integer> {
    private final TreeMap<Long, Node> nodeCircleMap = new TreeMap<Long, Node>();

    private RingHashPartitionedLoadBalancer(Set<Endpoint> nodes)
    {
      for (Endpoint endpoint : nodes)
      {
        Node node = endpoint.getNode();
        Set<Integer> partitionedIds = node.getPartitionIds();
          for (Integer partitionId : partitionedIds)
        {
          for (int r = 0; r < _numberOfReplicas; r++)
          {
            String distKey = node.getId() + ":" + partitionId + ":" + r + ":" + node.getUrl();
            nodeCircleMap.put(_hashingStrategy.hash(distKey), node);
          }
        }
      }
    }

    public Node nextNode(Integer partitionedId)
    {
      if (nodeCircleMap.isEmpty())
        return null;
      
      long hash = _hashingStrategy.hash(partitionedId.toString());
      if (!nodeCircleMap.containsKey(hash)) {
        Long k = nodeCircleMap.ceilingKey(hash);
        hash = (k == null) ? nodeCircleMap.firstKey() : k;
      }
      Node node = nodeCircleMap.get(hash);
      if (log.isDebugEnabled())
        log.debug(partitionedId + " is sent to node " + node.getId());
     return node;
    }

    @Override
    public Set<Node> nodesForOneReplica()
    {
       throw new UnsupportedOperationException("broad cast to entire replica not implemented in RingHashPartitionedLoadBalancerFactory");
    }
  }
  
  private final static double mean(int[] population)
  {
    double sum =0;
    for ( int x: population)
    {
      sum += x;
    }
    return sum / population.length;
  }
  
  private final static double variance(int[] population)
  {
    long n = 0;
    double mean = 0;
    double s = 0.0;
    
    for (int x : population)
    {
      n++;
      double delta = x - mean;
      mean += delta / n;
      s += delta * (x - mean);
    }
    return (s/n);
  }
  
  private final static double standard_deviation(int[] population)
  {
    return Math.sqrt(variance(population));
  }
}