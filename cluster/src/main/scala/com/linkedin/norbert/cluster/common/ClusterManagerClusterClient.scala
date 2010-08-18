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
import java.lang.String
import java.util.concurrent.{TimeUnit, CountDownLatch}
import jmx.JMX
import jmx.JMX.MBean
import notifications.{Observer, NotificationCenter}

trait ClusterManagerClusterClient extends ClusterClient {
  @volatile private var currentNodes = Set.empty[Node]
  @volatile private var availableNodes = Set.empty[Node]
  @volatile private var connectedLatch = new CountDownLatch(1)
  @volatile private var connectionException: Option[ClusterException] = None

  private var connectCalled = new AtomicBoolean
  private val shutdownSwitch = new AtomicBoolean
  private val notificationCenter = NotificationCenter.defaultNotificationCenter

  private val clusterManager = newClusterManager(new ClusterManagerDelegate {
    import ClusterEvents._

    def didShutdown = {
      notificationCenter.postNotification(Shutdown)
      //notificationCenter.shutdown
    }

    def didDisconnect = {
      connectedLatch = new CountDownLatch(1)
      connectionException = None
      currentNodes = Set.empty
      availableNodes = Set.empty
      notificationCenter.postNotification(Disconnected)
    }

    def nodesDidChange(nodes: Set[Node]) = {
      if (currentNodes != nodes) {
        currentNodes = nodes

        val available = currentNodes.filter(_.available)
        if (availableNodes != available) {
          availableNodes = available
          notificationCenter.postNotification(NodesChanged(availableNodes))
        }
      }
    }

    def didConnect(nodes: Set[Node]) = {
      connectionException = None
      currentNodes = nodes
      availableNodes = nodes.filter(_.available)
      connectedLatch.countDown
      notificationCenter.postNotification(Connected(availableNodes))
    }

    def connectionFailed(ex: ClusterException) = {
      connectionException = Some(ex)
      connectedLatch.countDown
    }
  })

  private val jmxHandle = JMX.register(new MBean(classOf[ClusterClientMBean], "serviceName=%s".format(serviceName)) with ClusterClientMBean {
    def isConnected = ClusterManagerClusterClient.this.isConnected
    def getNodes = currentNodes.map(_.toString).mkString("\n")
  })

  import ClusterManagerMessages._

  def connect = ifNotShutdown then {
    if (connectCalled.compareAndSet(false, true)) {
      log.debug("Connecting to cluster...")
      clusterManager.start
    } else {
      throw new AlreadyConnectedException
    }
  }

  def nodes = ifNotShutdown and ifConnectCalled then { currentNodes }

  def nodeWithId(nodeId: Int) = nodes.filter(_.id == nodeId).headOption

  def addNode(nodeId: Int, url: String, partitions: Set[Int]) = ifNotShutdown and ifConnectCalled then {
    val node = Node(nodeId, url, false, partitions)
    sendClusterManagerMessage(AddNode(node))
    node
  }

  def removeNode(nodeId: Int) = ifNotShutdown and ifConnectCalled then { sendClusterManagerMessage(RemoveNode(nodeId)) }

  def markNodeAvailable(nodeId: Int) = ifNotShutdown and ifConnectCalled then { sendClusterManagerMessage(MarkNodeAvailable(nodeId)) }

  def markNodeUnavailable(nodeId: Int) = ifNotShutdown and ifConnectCalled then { sendClusterManagerMessage(MarkNodeUnavailable(nodeId)) }

  def addListener(listener: ClusterListener) = ifNotShutdown then {
    val key = ClusterListenerKey(notificationCenter.addObserver(Observer {
      case e: ClusterEvent => listener.handleClusterEvent(e)
    }))

    if (connectCalled.get && isConnected) listener.handleClusterEvent(ClusterEvents.Connected(availableNodes))

    key
  }

  def removeListener(key: ClusterListenerKey) = ifNotShutdown then { notificationCenter.removeObserver(key.observerKey) }

  def isConnected = ifNotShutdown and ifConnectCalled then { connectedLatch.getCount == 0 && connectionException.isEmpty }

  def isShutdown = shutdownSwitch.get

  def awaitConnection = ifNotShutdown and ifConnectCalled then {
    connectedLatch.await
    connectionException.foreach(throw _)
    if (shutdownSwitch.get) throw new ClusterShutdownException
  }

  def awaitConnection(timeout: Long, unit: TimeUnit) = ifNotShutdown and ifConnectCalled then {
    val timedout = connectedLatch.await(timeout, unit)
    connectionException.foreach(throw _)
    if (shutdownSwitch.get) throw new ClusterShutdownException
    timedout
  }

  def awaitConnectionUninterruptibly = ifNotShutdown and ifConnectCalled then {
    var done = false

    while (!done) {
      try {
        awaitConnection
        done = true
      } catch {
        case ex: InterruptedException =>
      }
    }
  }

  def awaitConnectionUninterruptibly(timeout: Long, unit: TimeUnit) = ifNotShutdown and ifConnectCalled then {
    var done = false
    var timedout = false

    while (!done) {
      try {
        timedout = awaitConnection(timeout, unit)
        done = true
      } catch {
        case ex: InterruptedException =>
      }
    }

    timedout
  }

  def shutdown = if (shutdownSwitch.compareAndSet(false, true)) {
    log.debug("Shutting down ClusterClient")
    connectedLatch.countDown
    jmxHandle.foreach { JMX.unregister(_) }
    clusterManager ! ClusterManagerMessages.Shutdown
    log.info("ClusterClient shut down")
  } else {
    throw new ClusterShutdownException
  }

  private val ifConnectCalled = GuardChain(connectCalled.get, throw new NotYetConnectedException)
  private val ifNotShutdown = GuardChain(!shutdownSwitch.get, throw new ClusterShutdownException)

  protected def newClusterManager(delegate: ClusterManagerDelegate): ClusterManager

  private def sendClusterManagerMessage(message: ClusterManagerMessage) {
    clusterManager !? (250, message) match {
      case Some(ClusterManagerResponse(o)) => o.foreach { throw _ }
      case None => throw new ClusterException("Timed out waiting for response from the ClusterManager")
    }
  }
}
