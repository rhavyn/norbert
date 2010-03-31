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
package com.linkedin.norbert.network.javaapi;

import com.linkedin.norbert.cluster.ClusterDefaults;
import com.linkedin.norbert.cluster.javaapi.ClusterClient;
import com.linkedin.norbert.network.NetworkDefaults;

public class NetworkServerConfig {
  private ClusterClient clusterClient;
  private String serviceName;
  private String zooKeeperConnectString;
  private int zooKeeperSessionTimeoutMillis = ClusterDefaults.ZOOKEEPER_SESSION_TIMEOUT_MILLIS();
  private int requestThreadCorePoolSize = NetworkDefaults.REQUEST_THREAD_CORE_POOL_SIZE();
  private int requestThreadMaxPoolSize = NetworkDefaults.REQUEST_THREAD_MAX_POOL_SIZE();
  private int requestThreadKeepAliveTimeSecs = NetworkDefaults.REQUEST_THREAD_KEEP_ALIVE_TIME_SECS();

  public ClusterClient getClusterClient() {
    return clusterClient;
  }

  public void setClusterClient(ClusterClient clusterClient) {
    this.clusterClient = clusterClient;
  }

  public String getServiceName() {
    return serviceName;
  }

  public void setServiceName(String serviceName) {
    this.serviceName = serviceName;
  }

  public String getZooKeeperConnectString() {
    return zooKeeperConnectString;
  }

  public void setZooKeeperConnectString(String zooKeeperConnectString) {
    this.zooKeeperConnectString = zooKeeperConnectString;
  }

  public int getZooKeeperSessionTimeoutMillis() {
    return zooKeeperSessionTimeoutMillis;
  }

  public void setZooKeeperSessionTimeoutMillis(int zooKeeperSessionTimeoutMillis) {
    this.zooKeeperSessionTimeoutMillis = zooKeeperSessionTimeoutMillis;
  }

  public int getRequestThreadCorePoolSize() {
    return requestThreadCorePoolSize;
  }

  public void setRequestThreadCorePoolSize(int requestThreadCorePoolSize) {
    this.requestThreadCorePoolSize = requestThreadCorePoolSize;
  }

  public int getRequestThreadMaxPoolSize() {
    return requestThreadMaxPoolSize;
  }

  public void setRequestThreadMaxPoolSize(int requestThreadMaxPoolSize) {
    this.requestThreadMaxPoolSize = requestThreadMaxPoolSize;
  }

  public int getRequestThreadKeepAliveTimeSecs() {
    return requestThreadKeepAliveTimeSecs;
  }

  public void setRequestThreadKeepAliveTimeSecs(int requestThreadKeepAliveTimeSecs) {
    this.requestThreadKeepAliveTimeSecs = requestThreadKeepAliveTimeSecs;
  }
}
