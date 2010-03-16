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
package com.linkedin.norbert.cluster

import zookeeper.ZooKeeperClusterManagerComponent

/**
 * The default cluster implementation component mixin. Users should mix this component into their
 * component registry instead of pulling in the individual components. A <code>RouterFactory</code> implementation
 * must be provided.
 */
trait ZooKeeperClusterComponent extends ClusterComponent with ZooKeeperClusterManagerComponent {
  this: RouterFactoryComponent =>

  /**
   * The name of the cluster.
   */
  val clusterName: String

  /**
   * The URL string to use to connect to ZooKeeper.
   */
  val zooKeeperConnectString: String

  /**
   * The ZooKeeper session timeout in milliseconds.
   */
  val zooKeeperSessionTimeout: Int

  val cluster = newCluster

  private def newCluster: Cluster = {
    val cnm = new ClusterNotificationManager
    val zkm = new ZooKeeperClusterManager(zooKeeperConnectString, zooKeeperSessionTimeout, clusterName, cnm)
    new Cluster(cnm, zkm)
  }
}
