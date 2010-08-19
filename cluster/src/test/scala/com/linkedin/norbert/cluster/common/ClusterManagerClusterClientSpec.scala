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

class ClusterManagerClusterClientSpec extends ClusterManagerClusterClientSpecification {
  val clusterClient = new ClusterManagerClusterClient {
    def serviceName = "test"

    protected def newClusterManager(delegate: ClusterManagerDelegate) = {
      clusterManagerDelegate = delegate
      clusterManager = new TestClusterManager(delegate)
      clusterManager
    }
  }

  "An unconnected ClusterManagerClusterClient" should {
    doAfter {
      clusterManager ! ClusterManagerMessages.Shutdown
    }

    "behave like a ClusterManagerClusterClient" in { unconnectedExamples }
  }

  "A connected ClusterManagerClusterClient" should {
    doBefore {
      clusterClient.connect
    }

    doAfter {
      clusterManager ! ClusterManagerMessages.Shutdown
    }

    "behave like a ClusterManagerClusterClient" in { connectedExamples }
  }
}
