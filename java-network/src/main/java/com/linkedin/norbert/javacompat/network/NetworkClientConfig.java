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

import com.linkedin.norbert.cluster.ClusterDefaults;
import com.linkedin.norbert.javacompat.cluster.ClusterClient;
import com.linkedin.norbert.network.NetworkDefaults;

public class NetworkClientConfig {
  private ClusterClient clusterClient;
  private String serviceName;
  private String zooKeeperConnectString;
  private int zooKeeperSessionTimeoutMillis = ClusterDefaults.ZOOKEEPER_SESSION_TIMEOUT_MILLIS();
  private int connectTimeoutMillis = NetworkDefaults.CONNECT_TIMEOUT_MILLIS();
  private int writeTimeoutMillis = NetworkDefaults.WRITE_TIMEOUT_MILLIS();
  private int maxConnectionsPerNode = NetworkDefaults.MAX_CONNECTIONS_PER_NODE();
  private int staleRequestTimeoutMins = NetworkDefaults.STALE_REQUEST_TIMEOUT_MINS();
  private int staleRequestCleanupFrequencyMins = NetworkDefaults.STALE_REQUEST_CLEANUP_FREQUENCY_MINS();

  private long requestStatisticsWindow = NetworkDefaults.REQUEST_STATISTICS_WINDOW();

  private double outlierMuliplier = NetworkDefaults.OUTLIER_MULTIPLIER();
  private double outlierConstant = NetworkDefaults.OUTLIER_CONSTANT();

  private int responseHandlerCorePoolSize = NetworkDefaults.RESPONSE_THREAD_CORE_POOL_SIZE();
  private int responseHandlerMaxPoolSize = NetworkDefaults.RESPONSE_THREAD_MAX_POOL_SIZE();
  private int responseHandlerKeepAliveTime = NetworkDefaults.RESPONSE_THREAD_KEEP_ALIVE_TIME_SECS();
  private int responseHandlerMaxWaitingQueueSize = NetworkDefaults.RESPONSE_THREAD_POOL_QUEUE_SIZE();

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

  public int getConnectTimeoutMillis() {
    return connectTimeoutMillis;
  }

  public void setConnectTimeoutMillis(int connectTimeoutMillis) {
    this.connectTimeoutMillis = connectTimeoutMillis;
  }

  public int getWriteTimeoutMillis() {
    return writeTimeoutMillis;
  }

  public void setWriteTimeoutMillis(int writeTimeoutMillis) {
    this.writeTimeoutMillis = writeTimeoutMillis;
  }

  public int getMaxConnectionsPerNode() {
    return maxConnectionsPerNode;
  }

  public void setMaxConnectionsPerNode(int maxConnectionsPerNode) {
    this.maxConnectionsPerNode = maxConnectionsPerNode;
  }

  public int getStaleRequestTimeoutMins() {
    return staleRequestTimeoutMins;
  }

  public void setStaleRequestTimeoutMins(int staleRequestTimeoutMins) {
    this.staleRequestTimeoutMins = staleRequestTimeoutMins;
  }

  public int getStaleRequestCleanupFrequencyMins() {
    return staleRequestCleanupFrequencyMins;
  }

  public void setStaleRequestCleanupFrequencyMins(int staleRequestCleanupFrequencyMins) {
    this.staleRequestCleanupFrequencyMins = staleRequestCleanupFrequencyMins;
  }

  public long getRequestStatisticsWindow() {
    return requestStatisticsWindow;
  }

  public void setRequestStatisticsWindow(long requestStatisticsWindow) {
    this.requestStatisticsWindow = requestStatisticsWindow;
  }

  public double getOutlierMuliplier() {
    return outlierMuliplier;
  }

  public void setOutlierMuliplier(double outlierMuliplier) {
    this.outlierMuliplier = outlierMuliplier;
  }

  public double getOutlierConstant() {
    return outlierConstant;
  }

  public void setOutlierConstant(double outlierConstant) {
    this.outlierConstant = outlierConstant;
  }

  public int getResponseHandlerCorePoolSize() {
    return responseHandlerCorePoolSize;
  }

  public void setResponseHandlerCorePoolSize(int responseHandlerCorePoolSize) {
    this.responseHandlerCorePoolSize = responseHandlerCorePoolSize;
  }

  public int getResponseHandlerMaxPoolSize() {
    return responseHandlerMaxPoolSize;
  }

  public void setResponseHandlerMaxPoolSize(int responseHandlerMaxPoolSize) {
    this.responseHandlerMaxPoolSize = responseHandlerMaxPoolSize;
  }

  public int getResponseHandlerKeepAliveTime() {
    return responseHandlerKeepAliveTime;
  }

  public void setResponseHandlerKeepAliveTime(int responseHandlerKeepAliveTime) {
    this.responseHandlerKeepAliveTime = responseHandlerKeepAliveTime;
  }

  public int getResponseHandlerMaxWaitingQueueSize() {
    return responseHandlerMaxWaitingQueueSize;
  }

  public void setResponseHandlerMaxWaitingQueueSize(int responseHandlerMaxWaitingQueueSize) {
    this.responseHandlerMaxWaitingQueueSize = responseHandlerMaxWaitingQueueSize;
  }
}
