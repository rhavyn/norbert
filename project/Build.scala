import sbt._
import Keys._
import java.io.{FileInputStream, FileOutputStream}

object BuildSettings {
  val sonatypeRepo = "http://oss.sonatype.org/service/local/staging/deploy/maven2"

  lazy val credentialsSetting = credentials ++=
    (Seq("build.publish.user", "build.publish.password").map(k => Option(System.getProperty(k))) match {
      case Seq(Some(user), Some(pass)) =>
         Seq(Credentials("Sonatype Nexus Repository Manager", "oss.sonatype.org", user, pass))
      case _ =>
         Seq.empty[Credentials]
    })

  val buildSettings = Defaults.defaultSettings ++ Seq (
    organization := "com.linkedin",
    version      := "0.6.12",
    scalaVersion := "2.8.1",
    credentialsSetting,
    publishTo <<= (version) { version: String =>
      if (version.trim.endsWith("SNAPSHOT"))
        Some("Sonatype Nexus Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots")
      else
        Some("Sonatype Nexus Repository Manager" at "https://oss.sonatype.org/service/local/staging/deploy/maven2")
    }
  )
}

object Resolvers {
  val jbossRepo = "JBoss Maven 2 Repository" at "http://repository.jboss.org/nexus/content/groups/public/"
  val norbertResolvers = Seq(jbossRepo)
}

object ClusterDependencies {
  val ZOOKEEPER_VER = "3.3.0"
  val PROTOBUF_VER = "2.4.0a"
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
    val description = SettingKey[String]("description")
    val projects = Seq(cluster, network, javaCluster, javaNetwork)

    val myManagedSources = TaskKey[Seq[Seq[File]]]("my-managed-sources")
    val myUnmanagedSources = TaskKey[Seq[Seq[File]]]("my-unmanaged-sources")

    val mySettings = buildSettings

    def copyFile(input: File, output: File) {
      output.getParentFile.mkdirs
      output.createNewFile
        val inputChannel = new FileInputStream(input).getChannel
        val outputChannel = new FileOutputStream(output).getChannel
        outputChannel.transferFrom(inputChannel, 0, Long.MaxValue)
    }

    def copyProtoFiles(classDir: Types.Id[File]): File = {
      val protoClasses = (classDir ** "*Protos*.class").get

      val parentDir = new File(classDir.getParent)
      val protoTmpDir = parentDir / "proto-tmp"
      protoTmpDir.delete

      val rebased = protoClasses x rebase(oldBase = classDir, newBase = protoTmpDir)

      rebased.foreach {
        case (protoClass, newProtoClass) =>
          copyFile(protoClass, newProtoClass)
      }
      protoTmpDir
    }

    // Generating documentation fails because of our proto sources. This is a hacked up task to put them on the
    // classpath and proceed running with everything else.
    def filteredDocTask: Project.Initialize[Task[File]] =
      (classDirectory in Compile, cacheDirectory in Compile, compileInputs in Compile, streams in Compile, docDirectory in Compile, configuration in Compile , scaladocOptions in Compile)
      .map { (classDir, cache, in, s, target, config, options) =>
        val d = new Scaladoc(in.config.maxErrors, in.compilers.scalac)
        val cp = in.config.classpath.toList - in.config.classesDirectory
        val sources = in.config.sources
        val (protoSources, scalaSources) = sources.partition(file => file.getName.contains("Protos"))

        // add the java sources to the class path
        val protoTmpDir: File = copyProtoFiles(classDir)
        val classpath = protoTmpDir :: cp

        d.cached(cache / "doc", Defaults.nameForSrc(config.name), scalaSources, classpath , target, options, s.log)
        target
      }

    Project(
      id           = "norbert",
      base         = file("full"),
      settings     = mySettings ++ Seq(
        description := "Includes all of the norbert subprojects in one project",
        myManagedSources  <<= projects.map(managedSources in Compile in _).join,
        unmanagedClasspath in Compile += Attributed.blank(new java.io.File("doesnotexist")),
        myUnmanagedSources <<= projects.map(unmanagedSources in Compile in _).join,

        (unmanagedSources in Compile) <<= (myUnmanagedSources).map(_.flatten),
        (managedSources in Compile) <<= (myManagedSources).map(_.flatten),
        (unmanagedSourceDirectories in Compile) <<= (projects.map(unmanagedSourceDirectories in Compile in _).join).map(_.flatten),

        (doc in Compile) <<= filteredDocTask,

        pomExtra <<= (pomExtra, name, description) { (extra, name, desc) => extra ++ Seq(
          <name>{name}</name>,
          <description>{desc}</description>,
          <url>http://sna-projects.com/norbert</url>,
          <licenses>
            <license>
              <name>Apache</name>
              <url>http://github.com/linkedin-sna/norbert/raw/HEAD/LICENSE</url>
              <distribution>repo</distribution>
            </license>
          </licenses>,
          <scm>
            <url>http://github.com/linkedin-sna/norbert</url>
            <connection>scm:git:git://github.com/linkedin-sna/norbert.git</connection>
          </scm>,
          <developers>
            <developer>
              <id>jhartman</id>
              <name>Joshua Hartman</name>
              <url>http://http://twitter.com/hartmanster</url>
            </developer>
          </developers>
        )},

        libraryDependencies ++= ClusterDependencies.deps ++ NetworkDependencies.deps
      )
    )
  }

}
