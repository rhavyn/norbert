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
package zookeeper

import java.util.concurrent.{TimeUnit, TimeoutException}

class ZooKeeperClusterClientSpec extends ClusterClientSpecification {
  val clusterClient = new ZooKeeperClusterClient("test", "localhost:2181", 30000)

  "An unconnected ZooKeeperClusterClient" should {
    doAfter { cleanup }

    "behave like a ClusterClient" in { unconnectedClusterClientExamples }
  }

  "A shutdown ZooKeeperClusterClient" should {
    doBefore { clusterClient.shutdown }

    doAfter {
      try {
        cleanup
      } catch {
        case ex: ClusterShutdownException => // do nothing
      }
    }

    "behave like a ClusterClient" in { shutdownClusterClientExamples }
  }

  "A connected ZooKeeperClusterClient" should {
    doBefore {
      clusterClient.connect
      if (!clusterClient.awaitConnectionUninterruptibly(1, TimeUnit.SECONDS)) {
        throw new TimeoutException("Timed out waiting for connection to cluster")
      }

      (0 until 10).foreach { clusterClient.removeNode(_) }
      setup
    }

    doAfter {
      try {
        cleanup
      } catch {
        case ex: ClusterShutdownException => // do nothing
      }
    }

    "behave like a ClusterClient" in { connectedClusterClientExamples }
  }
}
