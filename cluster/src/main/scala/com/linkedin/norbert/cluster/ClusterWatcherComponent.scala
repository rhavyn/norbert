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

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{TimeUnit, ScheduledThreadPoolExecutor, ScheduledFuture}
import org.apache.zookeeper.{WatchedEvent, Watcher}
import com.linkedin.norbert.util.{NamedPoolThreadFactory, Logging}

/**
 * A component which provides the ZooKeeper <code>Watcher</code> instance used in Norbert.
 */
trait ClusterWatcherComponent {
  this: ZooKeeperMonitorComponent with ClusterManagerComponent =>
  
  val clusterWatcher: ClusterWatcher
  
  class ClusterWatcher(clusterDisconnectTimeout: Int) extends Watcher with Logging {
    @volatile private var initiallyConnected = false
    @volatile private var shutdownSwitch = false
    @volatile private var disconnectFuture: Option[ScheduledFuture[_]] = None
    private val wasDisconnected = new AtomicBoolean(true)
    private val scheduler = new ScheduledThreadPoolExecutor(1, new NamedPoolThreadFactory("cluster-watcher-scheduler"))

    def shutdown(): Unit = {
      log.ifDebug("ClusterWatcher shut down")
      shutdownSwitch = true
    }
    
    def process(event: WatchedEvent) {
      import Watcher.Event.{EventType, KeeperState}

      if (shutdownSwitch) return
      
      try {
        event.getType match {
          case EventType.None =>
            event.getState match {
              case KeeperState.SyncConnected =>
                val cancelled = cancelDisconnectTask
                val disconnected = wasDisconnected.getAndSet(false)
                log.ifInfo {
                  if (cancelled) "Reconnected to ZooKeeper" else "Connected to ZooKeeper"
                }
                zooKeeperMonitor.verifyStructure
                if (initiallyConnected && !disconnected) {
                  clusterManager ! ClusterMessages.NodesChanged(zooKeeperMonitor.currentNodes)
                } else {
                  clusterManager ! ClusterMessages.Connected(zooKeeperMonitor.currentNodes)
                  initiallyConnected = true
                }

              case KeeperState.Disconnected =>
                log.warn("Disconnected from ZooKeeper, starting %d second timer until cluster disconnect", clusterDisconnectTimeout)
                val f = scheduler.scheduleAtFixedRate(new Runnable {
                  def run = if (wasDisconnected.get) {
                    log.ifInfo("Unable to reconnect to ZooKeeper, retrying...")
                  } else {
                    wasDisconnected.set(true)
                    log.warn("Unable to communicate with ZooKeeper for %d seconds, disconnecting cluster", clusterDisconnectTimeout)
                    clusterManager ! ClusterMessages.Disconnected
                  }
                }, clusterDisconnectTimeout, 30000, TimeUnit.MILLISECONDS)
                disconnectFuture = Some(f)

              case KeeperState.Expired =>
                log.ifDebug("ZooKeeper session has expired")
                wasDisconnected.set(true)
                clusterManager ! ClusterMessages.Disconnected
                cancelDisconnectTask
                zooKeeperMonitor.reconnect
            }

          case EventType.NodeChildrenChanged => clusterManager ! ClusterMessages.NodesChanged(zooKeeperMonitor.currentNodes)
        }
      }
      catch {
        case ex: Exception => log.error(ex, "Unexpected exception")
      }
    }

    private def cancelDisconnectTask: Boolean = {
      disconnectFuture match {
        case Some(f) => if (f.isDone) {
          disconnectFuture = None
          false
        } else {
          log.ifDebug("Cancelling disconnect task")
          f.cancel(false)
          scheduler.purge
          true
        }

        case None => false
      }
    }
  }
}
