import sbt._

class NorbertProject(info: ProjectInfo) extends ParentProject(info) with IdeaPlugin {
  override def repositories = Set(ScalaToolsSnapshots, "JBoss Maven 2 Repository" at "http://repository.jboss.com/maven2")

  lazy val cluster = project("cluster", "Norbert Cluster", new ClusterProject(_))
  lazy val network = project("network", "Norbert Network", new NetworkProject(_), cluster)
  lazy val javaCluster = project("java-cluster", "Norbert Java Cluster", new DefaultProject(_) with IdeaPlugin, cluster)
  lazy val javaNetwork = project("java-network", "Norbert Java Network", new DefaultProject(_) with IdeaPlugin, javaCluster, network)
  lazy val examples = project("examples", "Norbert Examples", new DefaultProject(_) with IdeaPlugin, network, javaNetwork)

  class ClusterProject(info: ProjectInfo) extends DefaultProject(info) with IdeaPlugin {
    val zookeeper = "org.apache.hadoop" % "zookeeper" % "3.3.0" from "http://repo1.maven.org/maven2/org/apache/hadoop/zookeeper/3.3.0/zookeeper-3.3.0.jar"
    val protobuf = "com.google.protobuf" % "protobuf-java" % "2.3.0"
    val log4j = "log4j" % "log4j" % "1.2.14"

    val specs = "org.scala-tools.testing" %% "specs" % "1.6.5-SNAPSHOT" % "test"
    val mockito = "org.mockito" % "mockito-all" % "1.8.4" % "test"
    val cglib = "cglib" % "cglib" % "2.1_3" % "test"
    val objenesis = "org.objenesis" % "objenesis" % "1.0" % "test"
  }

  class NetworkProject(info: ProjectInfo) extends DefaultProject(info) with IdeaPlugin {
    val netty = "org.jboss.netty" % "netty" % "3.1.5.GA"
    val slf4j = "org.slf4j" % "slf4j-api" % "1.5.6"
    val slf4jLog4j = "org.slf4j" % "slf4j-log4j12" % "1.5.6"
  }
}
