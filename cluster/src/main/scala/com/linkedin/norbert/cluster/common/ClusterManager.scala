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
package cluster
package common

import actors.Actor
import logging.Logging

trait ClusterManager {
  this: Actor with Logging =>

  protected var currentNodes = Map.empty[Int, Node]
  protected val delegate: ClusterManagerDelegate

  def shutdown: Unit = this ! ClusterManagerMessages.Shutdown

  protected def nodeSet = Set.empty ++ currentNodes.values

  protected def invokeDelegate(f: => Unit) {
    try {
      f
    } catch {
      case ex: Exception => log.error(ex, "Delegate threw an exception")
    }
  }
}

sealed trait ClusterManagerMessage
object ClusterManagerMessages {
  case class AddNode(node: Node) extends ClusterManagerMessage
  case class RemoveNode(nodeId: Int) extends ClusterManagerMessage
  case class MarkNodeAvailable(nodeId: Int) extends ClusterManagerMessage
  case class MarkNodeUnavailable(nodeId: Int) extends ClusterManagerMessage
  case object GetNodes extends ClusterManagerMessage
  case class Nodes(nodes: Set[Node]) extends ClusterManagerMessage
  case object Shutdown extends ClusterManagerMessage
  case class ClusterManagerResponse(exception: Option[ClusterException]) extends ClusterManagerMessage
}

trait ClusterManagerDelegate {
  def connectionFailed(ex: Exception)
  def didConnect(nodes: Set[Node])
  def nodesDidChange(nodes: Set[Node])
  def didDisconnect: Unit
  def didShutdown: Unit
}
