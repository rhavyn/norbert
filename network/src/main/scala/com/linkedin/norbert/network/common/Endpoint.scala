package com.linkedin.norbert.network.common

import com.linkedin.norbert.cluster.Node

trait Endpoint {
  def node: Node

  def canServeRequests: Boolean
}