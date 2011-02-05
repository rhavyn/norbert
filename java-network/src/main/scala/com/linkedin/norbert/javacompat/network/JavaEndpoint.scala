package com.linkedin.norbert.javacompat.network

import com.linkedin.norbert.network.common.{Endpoint => SEndpoint}

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
  def getNode = endpoint.node
  def canServeRequests = endpoint.canServeRequests
}
