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

import com.linkedin.norbert.network.common.{Endpoint => SEndpoint}
import com.linkedin.norbert.cluster.{Node => SNode}

import javacompat.javaNodeToScalaNode
import javacompat.network.{JavaEndpoint, Endpoint => JEndpoint}

object EndpointConversions {
  implicit def scalaEndpointToJavaEndpoint(endpoint: SEndpoint): JEndpoint = {
    if (endpoint == null) null else JavaEndpoint(endpoint)
  }

  implicit def javaEndpointToScalaEndpoint(endpoint: JEndpoint): SEndpoint = {
    if(endpoint == null) null
    else new SEndpoint {
      def node: SNode = {
        javaNodeToScalaNode(endpoint.getNode)
      }

      def canServeRequests = endpoint.canServeRequests
    }
  }

  implicit def convertScalaEndpointSet(set: Set[SEndpoint]): java.util.Set[JEndpoint] = {
    val result = new java.util.HashSet[JEndpoint](set.size)
    set.foreach(elem => result.add(scalaEndpointToJavaEndpoint(elem)))
    result
  }

  implicit def convertJavaEndpointSet(set: java.util.Set[JEndpoint]): Set[SEndpoint] = {
    val iterator = set.iterator
    var sset = Set.empty[SEndpoint]
    while(iterator.hasNext) {
      sset += iterator.next
    }
    sset
  }
}
