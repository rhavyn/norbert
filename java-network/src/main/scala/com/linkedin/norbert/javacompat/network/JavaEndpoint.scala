package com.linkedin.norbert.javacompat.network

import com.linkedin.norbert.network.common.{Endpoint => SEndpoint}
import com.linkedin.norbert.javacompat.javaNodeToScalaNode
import com.linkedin.norbert.javacompat.cluster.JavaNode

object JavaEndpoint {
  def apply(endpoint: com.linkedin.norbert.network.common.Endpoint): JavaEndpoint = {
    if (endpoint == null) {
      null
    } else {
      new JavaEndpoint(endpoint)
    }
  }
}

class JavaEndpoint(endpoint: SEndpoint) extends Endpoint {
  def getNode = JavaNode(endpoint.node)
  def canServeRequests = endpoint.canServeRequests
}
