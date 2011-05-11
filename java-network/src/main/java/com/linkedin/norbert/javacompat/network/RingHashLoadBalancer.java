///*
//* Copyright 2009-2010 LinkedIn, Inc
//*
//* Licensed under the Apache License, Version 2.0 (the "License"); you may not
//* use this file except in compliance with the License. You may obtain a copy of
//* the License at
//*
//* http://www.apache.org/licenses/LICENSE-2.0
//*
//* Unless required by applicable law or agreed to in writing, software
//* distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
//* WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
//* License for the specific language governing permissions and limitations under
//* the License.
//*/
//package com.linkedin.norbert.javacompat.network;
//
//import java.util.*;
//
//import org.apache.log4j.Logger;
//
//import com.linkedin.norbert.javacompat.cluster.Node;
//
//public class RingHashLoadBalancer implements PartitionedLoadBalancer<Integer> {
//  private static Logger logger = Logger.getLogger(RingHashLoadBalancer.class);
//
//  private final TreeMap<Long, int[]> nodeCircleMap;
//  private final HashFunction<String> hashStrategy;
//  private final Set<Endpoint> endpoints;
//  private final Random random = new java.util.Random();
//
//  private static Map<Integer, Set<Endpoint>> invert(Set<Endpoint> endpoints) {
//    HashMap<Integer, Set<Endpoint>> pMap = new HashMap<Integer, Set<Endpoint>>();
//
//    for(Endpoint endpoint : endpoints) {
//      Node node = endpoint.getNode();
//
//      for(Integer partitionId : node.getPartitionIds()) {
//        Set<Endpoint> partitionEndpoints = pMap.get(partitionId) == null ? new HashSet<Endpoint>() : pMap.get(partitionId);
//        partitionEndpoints.add(endpoint);
//        pMap.put(partitionId, partitionEndpoints);
//      }
//    }
//    return pMap;
//  }
//
//  private static <K, V> int getSize(Map<K, ? extends Collection<V>> map) {
//    int size = 0;
//    for(Collection<V> v : map.values()) size += v.size();
//    return size;
//  }
//
//  public static RingHashLoadBalancer build(HashFunction<String> hashStrategy, int numReplicas, Set<Endpoint> endpoints) {
//    Map<Integer, Set<Endpoint>> pMap = invert(endpoints);
//    TreeMap<Long, int[]> nodeCircleMap = new TreeMap<Long, int[]>();
//
//    int slots = numReplicas * getSize(pMap);
//
//    Collection<Integer> partitions = pMap.keySet();
//    for(int r = 0; r < slots; r++) {
//      int[] nodeGroup = new int[partitions.size()];
//      int i = 0;
//      for(Integer partition : partitions) {
//        nodeGroup[i++] = r % pMap.get(partition).size();
//      }
//
//      String distKey = r + Arrays.toString(nodeGroup);
//      nodeCircleMap.put(hashStrategy.hash(distKey), nodeGroup);
//    }
//
//    return new RingHashLoadBalancer(hashStrategy, endpoints, nodeCircleMap);
//  }
//
//  private RingHashLoadBalancer(HashFunction<String> hashStrategy, Set<Endpoint> endpoints, TreeMap<Long, int[]> nodeCircleMap) {
//    this.hashStrategy = hashStrategy;
//    this.endpoints = endpoints;
//    this.nodeCircleMap = nodeCircleMap;
//  }
//
//  public Set<Integer> getPartitions() {
//    Set<Integer> result = new HashSet<Integer>();
//    for(Endpoint e : endpoints) {
//      result.addAll(e.getNode().getPartitionIds());
//    }
//    return result;
//  }
//
//  public int[] getNodeGroup(Integer id) {
//    if (nodeCircleMap.isEmpty())
//      return null;
//
//    long hash = hashStrategy.hash("" + id);
//    if (!nodeCircleMap.containsKey(hash)) {
//      Long k = nodeCircleMap.ceilingKey(hash);
//      hash = (k == null) ? nodeCircleMap.firstKey() : k;
//    }
//
//    int[] nodegroup = nodeCircleMap.get(hash);
//    if (logger.isDebugEnabled()) {
//      logger.debug(id + " is sent to node group " + Arrays.toString(nodegroup) + " for partitions: " + Arrays.toString(getPartitions()));
//    }
//    return nodegroup;
//  }
//
//  @Override
//  public Node nextNode(Integer id) {
//    int[] nodeGroup = getNodeGroup(id);
//    int idx = random.nextInt(nodeGroup.length);
//    return nodeGroup[i]
//  }
//
//  @Override
//  public Map<Node, Set<Integer>> nodesForOneReplica() {
//    throw new IllegalArgumentException();
//  }
//}
