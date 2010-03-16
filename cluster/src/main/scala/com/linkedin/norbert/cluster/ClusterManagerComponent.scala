package com.linkedin.norbert.cluster

trait ClusterManagerComponent {
  sealed trait ClusterManagerMessage
  object ClusterManagerMessages {
    case class AddNode(node: Node) extends ClusterManagerMessage
    case class RemoveNode(nodeId: Int) extends ClusterManagerMessage
    case class MarkNodeAvailable(nodeId: Int) extends ClusterManagerMessage
    case class MarkNodeUnavailable(nodeId: Int) extends ClusterManagerMessage

    case object Shutdown extends ClusterManagerMessage

    case class ClusterManagerResponse(exception: Option[ClusterException]) extends ClusterManagerMessage
  }
}
