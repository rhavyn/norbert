import java.io.File
import sbt._

class NorbertProject(info: ProjectInfo) extends ParentProject(info) with IdeaProject {
  override def repositories = Set(ScalaToolsSnapshots, "JBoss Maven 2 Repository" at "http://repository.jboss.org/nexus/content/groups/public/")

  lazy val cluster = project("cluster", "Norbert Cluster", new ClusterProject(_))
  lazy val network = project("network", "Norbert Network", new NetworkProject(_), cluster)
  lazy val javaCluster = project("java-cluster", "Norbert Java Cluster", new DefaultProject(_) with IdeaProject, cluster)
  lazy val javaNetwork = project("java-network", "Norbert Java Network", new DefaultProject(_) with IdeaProject, cluster, javaCluster, network)
  lazy val examples = project("examples", "Norbert Examples", new DefaultProject(_) with IdeaProject, network, javaNetwork)

  class ClusterProject(info: ProjectInfo) extends DefaultProject(info) with IdeaProject {
    val zookeeper = "org.apache.zookeeper" % "zookeeper" % "3.3.0" from "http://repo1.maven.org/maven2/org/apache/zookeeper/zookeeper/3.3.0/zookeeper-3.3.0.jar"
    val protobuf = "com.google.protobuf" % "protobuf-java" % "2.4.0a"
    val log4j = "log4j" % "log4j" % "1.2.16"

    val specs = "org.scala-tools.testing" %% "specs" % "1.6.7" % "test"
    val mockito = "org.mockito" % "mockito-all" % "1.8.4" % "test"
    val cglib = "cglib" % "cglib" % "2.1_3" % "test"
    val objenesis = "org.objenesis" % "objenesis" % "1.0" % "test"
  }

  class NetworkProject(info: ProjectInfo) extends DefaultProject(info) with IdeaProject {
    val netty = "org.jboss.netty" % "netty" % "3.2.3.Final"
    val slf4j = "org.slf4j" % "slf4j-api" % "1.5.6"
    val slf4jLog4j = "org.slf4j" % "slf4j-log4j12" % "1.5.6"
  }

  val oneJarName = artifactID + "-" + version + ".jar"

  lazy val oneJar = oneJarTask() dependsOn(publishLocal)

  def oneJarTask(): Task = task {
    FileUtilities.doInTemporaryDirectory(log) { temp: File =>
      projectClosure.dropRight(1).foreach { project =>
          println(project.outputPath)
          val files = (project.outputPath ** "*.jar").getFiles
          val paths = Path.fromFiles(files)
          paths.foreach(path => FileUtilities.unzip(path, Path.fromFile(temp), log))
      }

      Right(FileUtilities.zip(((Path.fromFile(temp) ##) ** "*.class").get,
                        outputPath / oneJarName, true, log))
    }.right.toOption.flatMap(x => x)
  }
}
