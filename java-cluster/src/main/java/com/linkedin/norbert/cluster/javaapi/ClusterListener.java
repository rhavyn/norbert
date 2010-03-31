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
package com.linkedin.norbert.cluster.javaapi;

import com.linkedin.norbert.cluster.Node;

public interface ClusterListener {
  /**
   * Handle the case that you are now connected to the cluster.
   *
   * @param nodes the current list of available <code>Node</code>s stored in the cluster metadata
   */
  void handleClusterConnected(Node[] nodes);

  /**
   * Handle the case that the cluster topology has changed.
   *
   * @param nodes the current list of available<code>Node</code>s stored in the cluster metadata
   */
  void handleClusterNodesChanged(Node[] nodes);

  /**
   * Handle the case that the cluster is now disconnected.
   */
  void handleClusterDisconnected();

  /**
   * Handle the case that the cluster is now shutdown.
   */
  void handleClusterShutdown();
}
