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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;

import org.apache.log4j.Logger;

import java.util.Arrays;

import com.linkedin.norbert.javacompat.cluster.Node;

public class RingHashLoadBalancer
{
  private static Logger logger = Logger.getLogger(RingHashLoadBalancer.class);
  private final TreeMap<Long, int[]> nodeCircleMap = new TreeMap<Long, int[]>();
  private final int _numberOfReplicas;
  private final HashFunction<String> _hashingStrategy;
  private final int[] _partitions;
  private final List<Node>[] _nodeslist;

  public RingHashLoadBalancer(HashFunction<String> hashingStrategy, int numberOfReplicas, Set<Node> nodes)
  {
    _hashingStrategy = hashingStrategy;
    _numberOfReplicas = numberOfReplicas;
    // pton is a mapping from partition ID to a list of nodes that serve that parition
    HashMap<Integer, List<Node>> pton = new HashMap<Integer, List<Node>>();
    int pnodecount = 0;
    for (Node node : nodes)
    {
      for (Integer partitionId : node.getPartitionIds())
      {
        List<Node> nodelist = pton.get(partitionId);
        if (nodelist == null)
        {
          nodelist = new ArrayList<Node>();
          pton.put(partitionId, nodelist);
        }
        if (!nodelist.contains(node))
        {
          nodelist.add(node);
          pnodecount ++;
        }
      }
    }
    int[] partitions = new int[pton.size()];
    Integer[] partitionset = pton.keySet().toArray(new Integer[0]);
    @SuppressWarnings("unchecked")
    List<Node>[] nodesarray = new List[partitions.length];
    for(int i = 0 ; i < partitionset.length; i++)
    {
      partitions[i] = partitionset[i];
      nodesarray[i] = pton.get(partitions[i]);
    }
    _partitions = partitions;
    _nodeslist = nodesarray;
    int slots = _numberOfReplicas * pnodecount;
    for (int r = 0; r < slots; r++)
    {
      int[] nodegroup = new int[_partitions.length];
      // nodegroup has a group of nodes that each serves a partition and they collectively serve all the partitions.
      for(int i = 0; i < _partitions.length; i++)
      {
        nodegroup[i] = r % _nodeslist[i].size();
      }
      String distKey = r + Arrays.toString(nodegroup);
      nodeCircleMap.put(_hashingStrategy.hash(distKey), nodegroup);
    }
  }

  public RoutingInfo route(int memberid)
  {
    if (nodeCircleMap.isEmpty())
      return null;

    long hash = _hashingStrategy.hash(""+memberid);
    if (!nodeCircleMap.containsKey(hash))
    {
      Long k = nodeCircleMap.ceilingKey(hash);
      hash = (k == null) ? nodeCircleMap.firstKey() : k;
    }
    int[] nodegroup = nodeCircleMap.get(hash);
    if (logger.isDebugEnabled())
    {
      logger.debug(memberid + " is sent to node group " + Arrays.toString(nodegroup) + " for parititions: " + Arrays.toString(_partitions));
    }
    return new RoutingInfo(_nodeslist, _partitions, nodegroup);
  }
  public static class RoutingInfo
  {
    public int [] partitions;
    public int [] nodegroup;
    public final List<Node>[] nodelist;
    public RoutingInfo(final List<Node>[] nodelist, int [] partitions, int[] nodegroup)
    {
      this.partitions = partitions;
      this.nodelist = nodelist;
      this.nodegroup = nodegroup;
    }
    public String toString()
    {
      StringBuffer sb = new StringBuffer();
      sb.append("Nodes: ").append(Arrays.toString(nodegroup)).append(" each for partitions: ").append(Arrays.toString(partitions));
      return sb.toString();
    }
  }
}
