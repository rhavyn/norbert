/*
 * Copyright 2009 LinkedIn, Inc
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
package com.linkedin.norbert.cluster

/**
 * The default cluster implementation component mixin. Users should mix this component into their
 * component registry instead of pulling in the individual components. A <code>RouterFactory</code> implementation
 * must be provided.
 */
trait DefaultClusterComponent extends ClusterComponent with ClusterManagerComponent
        with ClusterWatcherComponent with ZooKeeperMonitorComponent {
  this: RouterFactoryComponent =>

  /**
   * The name of the cluster.
   */
  val clusterName: String

  /**
   * The URL string to use to connect to ZooKeeper.
   */
  val zooKeeperUrls: String

  /**
   * The ZooKeeper session timeout in milliseconds. Defaults to 30000.
   */
  val zooKeeperSessionTimeout: Int = ClusterDefaults.ZOOKEEPER_SESSION_TIMEOUT

  /**
   * The cluster disconnect timeout in milliseconds. Defaults to 30000.
   */
  val clusterDisconnectTimeout: Int = ClusterDefaults.CLUSTER_DISCONNECT_TIMEOUT

  val clusterManager = new ClusterManager
  val clusterWatcher = new ClusterWatcher(clusterDisconnectTimeout)
  val zooKeeperMonitor = new ZooKeeperMonitor(zooKeeperUrls, zooKeeperSessionTimeout, clusterName)
  val cluster = Cluster()
}
