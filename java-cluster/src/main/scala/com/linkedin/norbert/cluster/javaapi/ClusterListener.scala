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
package com.linkedin.norbert.cluster.javaapi

import com.linkedin.norbert.cluster.Node

/**
 * A trait to be implemented by classes which wish to receive cluster events.  Register <code>ClusterListener</code>s
 * with <code>ClusterClient#addListener(listener)</code>.
 */
trait ClusterListener {
  /**
   * Handle the case that you are now connected to the cluster.
   *
   * @param nodes the current list of <code>Node</code>s stored in the cluster metadata
   * @param router a <code>Router</code> which is valid for the current state of the cluster
   */
  def handleClusterConnected(nodes: Array[Node], router: Router)

  /**
   * Handle the case that the cluster topology has changed.
   *
   * @param nodes the current list of <code>Node</code>s stored in the cluster metadata
   * @param router a <code>Router</code> which is valid for the current state of the cluster
   */
  def handleClusterNodesChanged(nodes: Array[Node], router: Router)

  /**
   * Handle the case that the cluster is now disconnected.
   */
  def handleClusterDisconnected()

  /**
   * Handle the case that the cluster is now shutdown.
   */
  def handleClusterShutdown()
}

/**
 * An implementation of <code>ClusterListener</code> with all do nothing implementations of the methods.
 * Users can subclass <code>ClusterListenerAdapter</code> and only override the methods they care about
 * instead of implementing <code>ClusterListener</code> directly.
 */
class ClusterListenerAdapter extends ClusterListener {
  def handleClusterConnected(nodes: Array[Node], router: Router) = null
  def handleClusterNodesChanged(nodes: Array[Node], router: Router) = null
  def handleClusterDisconnected() = null
  def handleClusterShutdown() = null
}
