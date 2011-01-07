package com.linkedin.norbert.javacompat.network;

import java.util.Set;
import java.util.HashSet;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import com.linkedin.norbert.cluster.InvalidClusterException;
import com.linkedin.norbert.javacompat.cluster.Node;
import com.linkedin.norbert.javacompat.cluster.JavaNode;
import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancer;
import com.linkedin.norbert.javacompat.network.PartitionedLoadBalancerFactory;

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
  public PartitionedLoadBalancer<Integer> newLoadBalancer(Set<Node> nodes) throws InvalidClusterException
  {
    return new RingHashPartitionedLoadBalancer(nodes);
  }
  
  private class RingHashPartitionedLoadBalancer implements PartitionedLoadBalancer<Integer> {
    private final TreeMap<Long, Node> nodeCircleMap = new TreeMap<Long, Node>();

    private RingHashPartitionedLoadBalancer(Set<Node> nodes)
    {
      for (Node node : nodes)
      {
        for (Integer partitionId : node.getPartitionIds())
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