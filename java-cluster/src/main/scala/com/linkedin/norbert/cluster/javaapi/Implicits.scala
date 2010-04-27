package com.linkedin.norbert.cluster.javaapi

import com.linkedin.norbert.cluster.javaapi.{Node => JNode}
import com.linkedin.norbert.cluster.{Node => SNode}

object Implicits {
  import collection.jcl.Conversions._

  implicit def scalaNodeToJavaNode(node: SNode): JNode = JavaNode(node)

  implicit def javaNodeToScalaNode(node: JNode): SNode = {
    val s = node.getPartitions.asInstanceOf[java.util.Set[Int]].foldLeft(Set[Int]()) { (set, id) => set + id }
    SNode(node.getId, node.getUrl, s, node.isAvailable)
  }

  implicit def scalaNodeSetToJavaNodeSet(nodes: Set[SNode]): java.util.Set[JNode] = {
    val s = new java.util.HashSet[JNode]
    nodes.foreach { n => s.add(n) }
    s
  }

  implicit def javaNodeSetToScalaNodeSet(nodes: java.util.Set[JNode]): Set[SNode] = {
    nodes.foldLeft(Set[SNode]()) { (set, n) => set + n }
  }
}
