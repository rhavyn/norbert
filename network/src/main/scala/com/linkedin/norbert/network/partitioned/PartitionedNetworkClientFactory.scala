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
package com.linkedin.norbert
package network
package partitioned

import loadbalancer.PartitionedLoadBalancerFactory
import client.NetworkClientConfig
import cluster.{ClusterClient}

/**
 * Author: jhartman
 */
class PartitionedNetworkClientFactory[PartitionedId](clientName: String,
                                                     serviceName: String,
                                                     zooKeeperConnectString: String,
                                                     zooKeeperSessionTimeoutMillis: Int,
                                                     norbertOutlierMultiplier: Double,
                                                     norbertOutlierConstant: Double,
                                                     partitionedLoadBalancerFactory: PartitionedLoadBalancerFactory[PartitionedId])
{

  def createPartitionedNetworkClient : PartitionedNetworkClient[PartitionedId] = {
    val config = new NetworkClientConfig

    config.outlierMuliplier = norbertOutlierMultiplier
    config.outlierConstant = norbertOutlierConstant
    config.clusterClient = ClusterClient(clientName, serviceName, zooKeeperConnectString, zooKeeperSessionTimeoutMillis)
    val partitionedNetworkClient = PartitionedNetworkClient(config, partitionedLoadBalancerFactory)

    partitionedNetworkClient
  }
}

