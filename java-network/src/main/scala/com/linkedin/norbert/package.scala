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
import javacompat.network.{JavaEndpoint, Endpoint => JEndpoint}
import collection.JavaConversions._

object EndpointConversions {
  implicit def scalaEndpointToJavaEndpoint(endpoint: SEndpoint): JEndpoint = {
    if (endpoint == null) null else JavaEndpoint(endpoint)
  }

  implicit def javaEndpointToScalaEndpoint(endpoint: JEndpoint): SEndpoint = {
    if(endpoint == null) null
    else new SEndpoint {
      def node = endpoint.node

      def canServeRequests = endpoint.canServeRequests
    }
  }

  implicit def convertScalaEndpointSet(set: Set[SEndpoint]): java.util.Set[JEndpoint] =
    set.map(node => scalaEndpointToJavaEndpoint(node))

  implicit def convertJavaEndpointSet(set: java.util.Set[JEndpoint]): Set[SEndpoint] =
    set.map(javaEndpointToScalaEndpoint(_)).toSet
}
