package com.linkedin.norbert.network.server

import com.linkedin.norbert.cluster.Node

trait ClusterIoServerComponent {
  val clusterIoServer: ClusterIoServer

  trait ClusterIoServer {
    def bind(node: Node, wildcardAddress: Boolean): Unit
    def shutdown: Unit
  }
}
