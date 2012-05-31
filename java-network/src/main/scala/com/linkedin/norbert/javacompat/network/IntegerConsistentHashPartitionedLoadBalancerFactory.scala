package com.linkedin.norbert.javacompat.network

class IntegerConsistentHashPartitionedLoadBalancerFactory(numPartitions: Int, serveRequestsIfPartitionMissing: Boolean)
        extends ConsistentHashPartitionedLoadBalancerFactory[Int](numPartitions, serveRequestsIfPartitionMissing) {
  def this(numPartitions: Int) = this(numPartitions, true)
  protected def hashPartitionedId(id: Int) =  id.hashCode
}