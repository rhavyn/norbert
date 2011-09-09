import sbt._
import Keys._

object BuildSettings {
  val buildSettings = Defaults.defaultSettings ++ Seq (
    organization := "com.linkedin.norbert",
    version      := "0.6.9",
    scalaVersion := "2.8.1",
    publishArtifact in (Compile, packageDoc) := false // For some reason, Scaladoc fails on the Protobuf classes we use
  )
}

object Resolvers {
  val jbossRepo = "JBoss Maven 2 Repository" at "http://repository.jboss.org/nexus/content/groups/public/"
  val norbertResolvers = Seq(jbossRepo)
}

object ClusterDependencies {
  val ZOOKEEPER_VER = "3.3.0"
  val PROTOBUF_VER = "2.4.1"
  val LOG4J_VER = "1.2.16"

  val SPECS_VER = "1.6.7"
  val MOCKITO_VER = "1.8.4"
  val CGLIB_VER = "2.1_3"
  val OBJENESIS = "1.0"

  val zookeeper = "org.apache.zookeeper" % "zookeeper" % ZOOKEEPER_VER

  val protobuf = "com.google.protobuf" % "protobuf-java" % PROTOBUF_VER

  val log4j = "log4j" % "log4j" % LOG4J_VER

  val specs = "org.scala-tools.testing" %% "specs" % SPECS_VER % "test"

  val mockito = "org.mockito" % "mockito-all" % MOCKITO_VER % "test"

  val cglib = "cglib" % "cglib" % CGLIB_VER % "test"

  val objenesis = "org.objenesis" % "objenesis" % OBJENESIS % "test"

  val deps = Seq(zookeeper, protobuf, log4j, specs, mockito, cglib, objenesis)
}

object NetworkDependencies {
  val NETTY_VER = "3.2.3.Final"
  val SLF4J_VER = "1.5.6"

  val netty = "org.jboss.netty" % "netty" % NETTY_VER
  val slf4j = "org.slf4j" % "slf4j-api" % SLF4J_VER
  val slf4jLog4j = "org.slf4j" % "slf4j-log4j12" % SLF4J_VER

  val deps = Seq(netty, slf4j, slf4jLog4j)
}

object NorbertBuild extends Build {
  import BuildSettings._
  import Resolvers._

  lazy val cluster = Project("cluster", file("cluster"),
    settings = buildSettings ++ Seq(libraryDependencies ++= ClusterDependencies.deps, resolvers := norbertResolvers))

  lazy val network = Project("network", file("network"),
    settings = buildSettings ++ Seq(libraryDependencies ++= NetworkDependencies.deps, resolvers := norbertResolvers)) dependsOn(cluster % "compile;test->test")

  lazy val javaCluster = Project("java-cluster", file("java-cluster"), settings = buildSettings) dependsOn(cluster)

  lazy val javaNetwork = Project("java-network", file("java-network"), settings = buildSettings) dependsOn(cluster, javaCluster, network)

  lazy val examples = Project("examples", file("examples"), settings = buildSettings) dependsOn(network, javaNetwork)

  lazy val root = Project("root", file("."), settings = buildSettings) aggregate(cluster, network, javaCluster, javaNetwork)

  lazy val full = {
    // The projects that are packaged in the full distribution.
    val projects = Seq(cluster, network, javaCluster, javaNetwork)

    val myManagedSources = TaskKey[Seq[Seq[File]]]("my-managed-sources")
    val myUnmanagedSources = TaskKey[Seq[Seq[File]]]("my-unmanaged-sources")

    Project(
      id           = "norbert",
      base         = file("full"),
      settings     = buildSettings ++ Seq(
        myManagedSources  <<= projects.map(managedSources in Compile in _).join,
        myUnmanagedSources <<= projects.map(unmanagedSources in Compile in _).join,

        (unmanagedSources in Compile) <<= (myUnmanagedSources).map(_.flatten),
        (managedSources in Compile) <<= (myManagedSources).map(_.flatten),
        (unmanagedSourceDirectories in Compile) <<= (projects.map(unmanagedSourceDirectories in Compile in _).join).map(_.flatten),

        libraryDependencies ++= ClusterDependencies.deps ++ NetworkDependencies.deps
      )
    )
  }

}