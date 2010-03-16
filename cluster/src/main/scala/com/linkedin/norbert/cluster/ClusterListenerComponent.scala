package com.linkedin.norbert.cluster


trait ClusterListenerComponent {
  sealed trait ClusterEvent
  object ClusterEvents {
    /**
     * <code>ClusterEvent</code> which indicates that you are now connected to the cluster.
     *
     * @param nodes the current list of <code>Node</code>s stored in the cluster metadata
     * @param router a <code>Router</code> which is valid for the current state of the cluster
     */
    case class Connected(nodes: Seq[Node]) extends ClusterEvent

    /**
     * <code>ClusterEvent</code> which indicates that the cluster topology has changed.
     *
     * @param nodes the current list of <code>Node</code>s stored in the cluster metadata
     * @param router a <code>Router</code> which is valid for the current state of the cluster
     */
    case class NodesChanged(nodes: Seq[Node]) extends ClusterEvent

    /**
     * <code>ClusterEvent</code> which indicates that the cluster is now disconnected.
     */
    case object Disconnected extends ClusterEvent

    /**
     * <code>ClusterEvent</code> which indicates that the cluster is now shutdown.
     */
    case object Shutdown extends ClusterEvent
  }

  /**
   * A trait to be implemented by classes which wish to receive cluster events.  Register <code>ClusterListener</code>s
   * with <code>Cluster#addListener(listener)</code>.
   */
  trait ClusterListener {
    /**
     * Handle a cluster event.
     *
     * @param event the <code>ClusterEvent</code> to handle
     */
    def handleClusterEvent(event: ClusterEvent): Unit
  }

  case class ClusterListenerKey(id: Long)
}
