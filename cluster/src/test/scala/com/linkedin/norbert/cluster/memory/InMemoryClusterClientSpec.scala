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
package memory

import org.specs.Specification

class InMemoryClusterClientSpec extends Specification {
  val clusterClient = new InMemoryClusterClient("test")
  clusterClient.start
  clusterClient.awaitConnectionUninterruptibly

  "InMemoryClusterClient" should {
    "start with no nodes" in {
      clusterClient.nodes.size must be_==(0)
    }

    "add the node" in {
      clusterClient.addNode(1, "test") must notBeNull
      val nodes = clusterClient.nodes
      nodes.size must be_==(1)
      nodes.foreach { node =>
        node.id must be_==(1)
        node.url must be_==("test")
        node.available must beFalse
      }
    }

    "throw an InvalidNodeException if the node already exists" in {
      clusterClient.addNode(1, "test") must notBeNull
      clusterClient.addNode(1, "test") must throwA[InvalidNodeException]
    }

    "add the node as available" in {
      clusterClient.markNodeAvailable(1)
      clusterClient.addNode(1, "test")
      val nodes = clusterClient.nodes
      nodes.foreach { node =>
        node.available must beTrue
      }
    }

    "remove the node" in {
      clusterClient.addNode(1, "test")
      clusterClient.nodes.size must be_==(1)
      clusterClient.removeNode(1)
      clusterClient.nodes.size must be_==(0)
    }

    "mark the node available" in {
      clusterClient.addNode(1, "test")
      var nodes = clusterClient.nodes
      nodes.foreach { node =>
        node.available must beFalse
      }
      clusterClient.markNodeAvailable(1)
      nodes = clusterClient.nodes
      nodes.foreach { node =>
        node.available must beTrue
      }
    }

    "mark the node unavailable" in {
      clusterClient.markNodeAvailable(1)
      clusterClient.addNode(1, "test")
      var nodes = clusterClient.nodes
      nodes.foreach { node =>
        node.available must beTrue
      }
      clusterClient.markNodeUnavailable(1)
      nodes = clusterClient.nodes
      nodes.foreach { node =>
        node.available must beFalse
      }
    }
  }
}
