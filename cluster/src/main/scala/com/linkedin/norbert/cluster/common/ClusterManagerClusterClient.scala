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

import util.GuardChain
import java.util.concurrent.atomic.AtomicBoolean
import collection.immutable.Set
import java.lang.String
import java.util.concurrent.{TimeUnit, CountDownLatch}
import jmx.JMX
import jmx.JMX.MBean

trait ClusterManagerClusterClient extends ClusterClient {
  this: ClusterManagerComponent =>

  private val shutdownSwitch = new AtomicBoolean
  @volatile private var connectedLatch = new CountDownLatch(1)
  @volatile private var connectCalled = false

  private val jmxHandle = JMX.register(new MBean(classOf[ClusterClientMBean], "serviceName=%s".format(serviceName)) with ClusterClientMBean {
    def isConnected = ClusterManagerClusterClient.this.isConnected

    def getNodes = try {
      nodes.map(_.toString).mkString("\n")
    } catch {
      case ex: Exception => "Error: " + ex.getCause
    }
  })

  import NotificationCenterMessages._
  import ClusterManagerMessages._

  def connect = ifNotShutdown then {
    log.debug("Connecting to cluster...")
    notificationCenter !? AddListener(ClusterListener {
      case ClusterEvents.Connected(_) => connectedLatch.countDown
      case ClusterEvents.Disconnected => connectedLatch = new CountDownLatch(1)
    })

    clusterManager !? Connect match { case ClusterManagerResponse(o) => o.foreach { throw _ } }
    connectCalled = true
    log.info("Connected to cluster")
  }

  def nodes = ifConnectCalled and ifNotShutdown then {
    clusterManager !? GetNodes match {
      case Nodes(n) => n
      case ClusterManagerResponse(o) => throw o.getOrElse { new ClusterException("Invalid response from ClusterManager") }
    }
  }

  def nodeWithId(nodeId: Int) = nodes.filter(_.id == nodeId).headOption

  def addNode(nodeId: Int, url: String, partitions: Set[Int]) = ifConnectCalled and ifNotShutdown then {
    val node = Node(nodeId, url, false, partitions)
    sendClusterManagerMessage(AddNode(node))
    node
  }

  def removeNode(nodeId: Int) = ifConnectCalled and ifNotShutdown then { sendClusterManagerMessage(RemoveNode(nodeId)) }

  def markNodeAvailable(nodeId: Int) = ifConnectCalled and ifNotShutdown then { sendClusterManagerMessage(MarkNodeAvailable(nodeId)) }

  def markNodeUnavailable(nodeId: Int) = ifConnectCalled and ifNotShutdown then { sendClusterManagerMessage(MarkNodeUnavailable(nodeId)) }

  def addListener(listener: ClusterListener) = ifNotShutdown then {
    notificationCenter !? AddListener(listener) match {
      case AddedListener(key) => key
    }
  }

  def removeListener(key: ClusterListenerKey) = ifNotShutdown then { notificationCenter ! RemoveListener(key) }

  def isConnected = ifConnectCalled and ifNotShutdown then { connectedLatch.getCount == 0 }

  def isShutdown = shutdownSwitch.get

  def awaitConnection = ifConnectCalled and ifConnectCalled and ifNotShutdown then { connectedLatch.await }

  def awaitConnection(timeout: Long, unit: TimeUnit) = ifConnectCalled and ifNotShutdown then { connectedLatch.await(timeout, unit) }

  def awaitConnectionUninterruptibly = ifConnectCalled and ifNotShutdown then {
    var done = false

    while (!done) {
      try {
        connectedLatch.await
        done = true
      } catch {
        case ex: InterruptedException =>
      }
    }
  }

  def awaitConnectionUninterruptibly(timeout: Long, unit: TimeUnit) = ifConnectCalled and ifNotShutdown then {
    var done = false
    var timedout = false

    while (!done) {
      try {
        timedout = connectedLatch.await(timeout, unit)
        done = true
      } catch {
        case ex: InterruptedException =>
      }
    }

    timedout
  }

  def shutdown = if (shutdownSwitch.compareAndSet(false, true)) {
    log.debug("Shutting down ClusterClient")

    jmxHandle.foreach { JMX.unregister(_) }
    notificationCenter !? NotificationCenterMessages.Shutdown
    clusterManager !? ClusterManagerMessages.Shutdown

    log.info("ClusterClient shut down")
  } else {
    throw new ClusterShutdownException
  }

  private val ifConnectCalled = GuardChain(connectCalled, throw new NotYetConnectedException)
  private val ifNotShutdown = GuardChain(!shutdownSwitch.get, throw new ClusterShutdownException)

  private def sendClusterManagerMessage(message: ClusterManagerMessage) {
    clusterManager !? message match { case ClusterManagerResponse(o) => o.foreach { throw _ } }
  }
}
