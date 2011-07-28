package com.linkedin.norbert.network.partitioned.loadbalancer

import com.linkedin.norbert.network.common.Endpoint
import java.util.TreeMap
import com.linkedin.norbert.cluster.{Node, InvalidClusterException}

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

/**
 * Similar to both the DefaultPartitionedLoadBalancer and  the SimpleConsistentHashedLoadBalancer, but uses a wheel per partition.
 * This means that if you shard your data according to some key but maintain replicas, rather than load balancing between the replicas,
 * the replicas will also use consistent hashing. This can be useful for a distributed database like system where you'd like
 * to shard the data by key, but you'd also like to maximize cache utilization for the replicas.
 */
class PartitionedConsistentHashedLoadBalancerFactory[PartitionedId](numPartitions: Int,
                                                                    numReplicas: Int,
                                                                    hashFn: PartitionedId => Int,
                                                                    endpointHashFn: String => Int,
                                                                    serveRequestsIfPartitionMissing: Boolean) extends PartitionedLoadBalancerFactory[PartitionedId] {
  def this(slicesPerEndpoint: Int, hashFn: PartitionedId => Int, endpointHashFn: String => Int, serveRequestsIfPartitionMissing: Boolean) = {
    this(-1, slicesPerEndpoint, hashFn, endpointHashFn, serveRequestsIfPartitionMissing)
  }

  @throws(classOf[InvalidClusterException])
  def newLoadBalancer(endpoints: Set[Endpoint]): PartitionedConsistentHashedLoadBalancer[PartitionedId] = {

    val partitions = endpoints.foldLeft(Map.empty[Int, Set[Endpoint]]) { (map, endpoint) =>
      endpoint.node.partitionIds.foldLeft(map) { (map, partition) =>
        map + (partition -> (map.getOrElse(partition, Set.empty[Endpoint]) + endpoint))
      }
    }

    val wheels = partitions.map { case (partition, endpointsForPartition) =>
      val wheel = new TreeMap[Int, Endpoint]
      endpointsForPartition.foreach { endpoint =>
        endpoint.node.partitionIds.foreach { partitionId =>
          (0 until numReplicas).foreach { r =>
            val node = endpoint.node
            var distKey = node.id + ":" + partitionId + ":" + r + ":" + node.url
            wheel.put(endpointHashFn(distKey), endpoint)
          }
        }
      }
      (partition, wheel)
    }

    val nPartitions = if(this.numPartitions == -1) endpoints.flatMap(_.node.partitionIds).size else numPartitions
    new PartitionedConsistentHashedLoadBalancer(nPartitions, wheels, hashFn, serveRequestsIfPartitionMissing)
  }
}

class PartitionedConsistentHashedLoadBalancer[PartitionedId](numPartitions: Int, wheels: Map[Int, TreeMap[Int, Endpoint]], hashFn: PartitionedId => Int, serveRequestsIfPartitionMissing: Boolean = true)
        extends PartitionedLoadBalancer[PartitionedId] with DefaultLoadBalancerHelper {
  val random = new scala.util.Random
  import collection.JavaConversions._
  val endpoints = wheels.values.flatMap(_.values).toSet
  val partitionToNodeMap = generatePartitionToNodeMap(endpoints, numPartitions, serveRequestsIfPartitionMissing)

  def nodesForOneReplica = {
    partitionToNodeMap.keys.foldLeft(Map.empty[Node, Set[Int]]) { (map, partition) =>
      val nodeOption = nodeForPartition(partition)
      if(nodeOption.isDefined) {
        val n = nodeOption.get
        map + (n -> (map.getOrElse(n, Set.empty[Int]) + partition))
      } else if(serveRequestsIfPartitionMissing) {
        log.warn("Partition %s is unavailable, attempting to continue serving requests to other partitions.".format(partition))
        map
      } else
        throw new InvalidClusterException("Partition %s is unavailable, cannot serve requests.".format(partition))
    }
  }

  def nextNode(id: PartitionedId): Option[Node] = {
    val hash = hashFn(id)
    val partitionId = hash.abs % numPartitions
    wheels.get(partitionId).flatMap { wheel =>
      PartitionUtil.searchWheel(wheel, hash, (e: Endpoint) => e.canServeRequests)
    }.map(_.node)
  }

  def partitionForId(id: PartitionedId): Int = {
    hashFn(id).abs % numPartitions
  }
}