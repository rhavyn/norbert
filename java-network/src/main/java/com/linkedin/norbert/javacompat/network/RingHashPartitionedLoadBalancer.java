package com.linkedin.norbert.javacompat.network;

import com.linkedin.norbert.javacompat.cluster.Node;
import org.apache.log4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class RingHashPartitionedLoadBalancer implements PartitionedLoadBalancer<Integer> {
  public static final Logger log = Logger.getLogger(RingHashPartitionedLoadBalancer.class);

  private final TreeMap<Long, Node> nodeCircleMap = new TreeMap<Long, Node>();
  private final HashFunction<String> hashStrategy;

  RingHashPartitionedLoadBalancer(int numReplicas, Set<Endpoint> nodes, HashFunction<String> hashingStrategy) {
    hashStrategy = hashingStrategy;
    for (Endpoint endpoint : nodes) {
      Node node = endpoint.getNode();
      Set<Integer> partitionedIds = node.getPartitionIds();
      for (Integer partitionId : partitionedIds) {
        for (int r = 0; r < numReplicas; r++) {
          String distKey = node.getId() + ":" + partitionId + ":" + r + ":" + node.getUrl();
          nodeCircleMap.put(hashingStrategy.hash(distKey), node);
        }
      }
    }
  }

  public Node nextNode(Integer partitionedId) {
    if (nodeCircleMap.isEmpty())
      return null;

    long hash = hashStrategy.hash(partitionedId.toString());
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
  public Map<Node, Set<Integer>> nodesForOneReplica() {
    throw new UnsupportedOperationException("broad cast to entire replica not implemented in RingHashPartitionedLoadBalancerFactory");
  }
}