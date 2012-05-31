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
package network
package server

import org.specs.Specification
import org.specs.mock.Mockito
import cluster._
import network.common.SampleMessage

class NetworkServerSpec extends Specification with Mockito with SampleMessage {
  val networkServer = new NetworkServer with ClusterClientComponent with ClusterIoServerComponent with MessageHandlerRegistryComponent
      with MessageExecutorComponent {
    val clusterIoServer = mock[ClusterIoServer]
    val clusterClient = mock[ClusterClient]
    val messageHandlerRegistry = mock[MessageHandlerRegistry]
    val messageExecutor = null
  }

  val node = Node(1, "", false)
  val listenerKey: ClusterListenerKey = ClusterListenerKey(1)
  networkServer.clusterClient.nodeWithId(1) returns Some(node)
  networkServer.clusterClient.addListener(any[ClusterListener]) returns listenerKey

  "NetworkServer" should {
    "throw a NetworkShutdownException if the network has been shutdown" in {
      networkServer.shutdown
      networkServer.bind(1) must throwA[NetworkShutdownException]
      networkServer.bind(1, false) must throwA[NetworkShutdownException]
      networkServer.markAvailable must throwA[NetworkShutdownException]
      networkServer.markUnavailable must throwA[NetworkShutdownException]
      networkServer.myNode must throwA[NetworkShutdownException]
    }

    "throw a NetworkServerNotBound exception if bind has not be called" in {
      networkServer.myNode must throwA[NetworkServerNotBoundException]
      networkServer.markUnavailable must throwA[NetworkServerNotBoundException]
      networkServer.markAvailable must throwA[NetworkServerNotBoundException]
    }

    "register messages with the MessageHandlerRegistry" in {
      doNothing.when(networkServer.messageHandlerRegistry).registerHandler((ping: Ping) => new Ping)

      networkServer.registerHandler((ping : Ping) => new Ping)
    }

    "when bind is called" in {
      "start the cluster client, await the completion and register as a listener" in {
        doNothing.when(networkServer.clusterClient).start
        doNothing.when(networkServer.clusterClient).awaitConnectionUninterruptibly

        networkServer.bind(1)

        got {
          one(networkServer.clusterClient).start
          one(networkServer.clusterClient).awaitConnectionUninterruptibly
          one(networkServer.clusterClient).addListener(any[ClusterListener])
        }
      }

      "bind to the socket" in {
        doNothing.when(networkServer.clusterIoServer).bind(node, true)

        networkServer.bind(1)

        there was one(networkServer.clusterIoServer).bind(node, true)
      }

      "throw an InvalidNodeException if the nodeId doesn't exist" in {
        networkServer.clusterClient.nodeWithId(1) returns None

        networkServer.bind(1) must throwA[InvalidNodeException]
      }

      "if markAvailable is true, mark the node available when a Connection message is received" in {
        var listener: ClusterListener = null
        networkServer.clusterClient.addListener(any[ClusterListener]) answers { l => listener = l.asInstanceOf[ClusterListener]; ClusterListenerKey(1) }
        doNothing.when(networkServer.clusterClient).markNodeAvailable(1)

        networkServer.bind(1)

        listener.handleClusterEvent(ClusterEvents.Connected(Set()))

        there was one(networkServer.clusterClient).markNodeAvailable(1)
      }

      "if markAvailable is false, not mark the node available when a Connection message is received" in {
        var listener: ClusterListener = null
        networkServer.clusterClient.addListener(any[ClusterListener]) answers { l => listener = l.asInstanceOf[ClusterListener]; ClusterListenerKey(1) }
        doNothing.when(networkServer.clusterClient).markNodeAvailable(1)

        networkServer.bind(1, false)

        listener.handleClusterEvent(ClusterEvents.Connected(Set()))

        there was no(networkServer.clusterClient).markNodeAvailable(1)
      }
    }

    "return the node associated with the nodeId bound to for myNode" in {
      networkServer.bind(1)

      networkServer.myNode must be_==(node)
    }

    "mark the node available and ensure it stays available when Connected events are received for markAvailable" in {
      var listener: ClusterListener = null
      networkServer.clusterClient.addListener(any[ClusterListener]) answers { l => listener = l.asInstanceOf[ClusterListener]; ClusterListenerKey(1) }
      doNothing.when(networkServer.clusterClient).markNodeAvailable(1)

      networkServer.bind(1, false)

      listener.handleClusterEvent(ClusterEvents.Connected(Set()))

      networkServer.markAvailable

      there was one(networkServer.clusterClient).markNodeAvailable(1)

      listener.handleClusterEvent(ClusterEvents.Connected(Set()))

      there were two(networkServer.clusterClient).markNodeAvailable(1)
    }

    "mark the node unavailable and ensure it is not marked available when Connected events are received for markUnavailable" in {
      var listener: ClusterListener = null
      networkServer.clusterClient.addListener(any[ClusterListener]) answers { l => listener = l.asInstanceOf[ClusterListener]; ClusterListenerKey(1) }
      doNothing.when(networkServer.clusterClient).markNodeAvailable(1)
      doNothing.when(networkServer.clusterClient).markNodeUnavailable(1)

      networkServer.bind(1)

      listener.handleClusterEvent(ClusterEvents.Connected(Set()))

      there was one(networkServer.clusterClient).markNodeAvailable(1)

      networkServer.markUnavailable

      listener.handleClusterEvent(ClusterEvents.Connected(Set()))

      got {
        one(networkServer.clusterClient).markNodeAvailable(1)
        one(networkServer.clusterClient).markNodeUnavailable(1)
      }
    }

    "shutdown the cluster io server, mark unavailable, and remove the cluster listener if shutdown is called" in {
      doNothing.when(networkServer.clusterIoServer).shutdown
      doNothing.when(networkServer.clusterClient).markNodeUnavailable(1)
      doNothing.when(networkServer.clusterClient).removeListener(listenerKey)

      networkServer.bind(1)
      networkServer.shutdown

      got {
        one(networkServer.clusterIoServer).shutdown
        one(networkServer.clusterClient).markNodeUnavailable(1)
        one(networkServer.clusterClient).removeListener(listenerKey)
      }
    }

    "shutdown the cluster io server if a Shutdown event is received" in {
      var listener: ClusterListener = null
      networkServer.clusterClient.addListener(any[ClusterListener]) answers { l => listener = l.asInstanceOf[ClusterListener]; ClusterListenerKey(1) }
      doNothing.when(networkServer.clusterIoServer).shutdown
      doNothing.when(networkServer.clusterClient).markNodeUnavailable(1)
      doNothing.when(networkServer.clusterClient).removeListener(listenerKey)

      networkServer.bind(1)
      listener.handleClusterEvent(ClusterEvents.Shutdown)

      there was one(networkServer.clusterIoServer).shutdown
      there was no(networkServer.clusterClient).markNodeUnavailable(1)
      there was no(networkServer.clusterClient).removeListener(listenerKey)
    }
  }
}
