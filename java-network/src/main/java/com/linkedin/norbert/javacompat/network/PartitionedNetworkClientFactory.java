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

public class PartitionedNetworkClientFactory<PartitionedId> {

  private final NetworkClientConfig _config;
  private final PartitionedLoadBalancerFactory<PartitionedId> _partitionedLoadBalancerFactory;

  public PartitionedNetworkClientFactory(String serviceName,
                                           String zooKeeperConnectString,
                                           Integer zooKeeperSessionTimeoutMillis,
                                           Double norbertOutlierMultiplier,
                                           Double norbertOutlierConstant,
                                           PartitionedLoadBalancerFactory<PartitionedId> partitionedLoadBalancerFactory)
  {
    _config = new NetworkClientConfig();
    _config.setServiceName(serviceName);
    _config.setZooKeeperConnectString(zooKeeperConnectString);
    _config.setZooKeeperSessionTimeoutMillis(zooKeeperSessionTimeoutMillis);
    _config.setOutlierMuliplier(norbertOutlierMultiplier);
    _config.setOutlierConstant(norbertOutlierConstant);
    _partitionedLoadBalancerFactory = partitionedLoadBalancerFactory;
  }


  public PartitionedNetworkClient<PartitionedId> createPartitionedNetworkClient()
  {
    return new NettyPartitionedNetworkClient<PartitionedId>(_config,
                                                       _partitionedLoadBalancerFactory);
  }
}
