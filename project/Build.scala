import sbt._
import Keys._

object SblajBuild extends Build {
  
  lazy val root = Project(id = "root", base = file("."), settings = rootSettings) aggregate (core, ml, spark)

  lazy val core = Project("core", file("core"), settings = coreSettings)

  lazy val ml = Project("ml", file("ml"), settings = mlSettings) dependsOn (core)
  lazy val spark = Project("spark", file("spark"), settings = sparkSettings) dependsOn (core)

  lazy val examples = Project("examples", file("examples"), settings = examplesSettings) dependsOn(core, ml, spark)

  val qf = "http://repo.quantifind.com/content/repositories/"
  val qfExtSnapshotRepo = "Quantifind External Snapshots" at "http://repo.quantifind.com/content/repositories/ext-snapshots/"

  def sharedSettings = Defaults.defaultSettings ++ Seq(
    organization := "org.sblaj",
    version := "0.1-SNAPSHOT",
    scalaVersion := "2.10.4",
    scalacOptions := Seq("-deprecation", "-unchecked", "-optimize"),
    unmanagedJars in Compile <<= baseDirectory map { base => (base / "lib" ** "*.jar").classpath },
    retrieveManaged := true,
    transitiveClassifiers in Scope.GlobalScope := Seq("sources"),
    publishTo <<= version {
      (v: String) =>
        Some("snapshots" at qf + "ext-snapshots")
    },
    resolvers ++= Seq(
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/",
      qfExtSnapshotRepo
    ),
    libraryDependencies ++= Seq(
      "org.eclipse.jetty" % "jetty-server" % "7.5.3.v20111011",
      "org.scalatest" %% "scalatest" % "2.1.3" % "test",
      "org.scalacheck" %% "scalacheck" % "1.11.3" % "test",
      "it.unimi.dsi" % "fastutil" % "6.4.4"  //better collections. its a big external dependency, but i think its worth it
    )
  )

  val slf4jVersion = "1.6.1"

  def rootSettings = sharedSettings ++ Seq(
    publish := {}
  )

  def coreSettings = sharedSettings ++ Seq(
    name := "sblaj-core",
    libraryDependencies ++= Seq(
      "log4j" % "log4j" % "1.2.16",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion,
      // we use protobuf just for the varint readers
      "com.google.protobuf" % "protobuf-java" % "2.3.0"
    )
  )

  def mlSettings = sharedSettings ++ Seq(
    name := "sblaj-ml"
  )

  def sparkSettings = sharedSettings ++ Seq(
    name := "sblaj-spark",
    libraryDependencies ++= Seq(
      "org.apache.spark" % "spark-core_2.10" % "0.8.0.99e517d"
    ),
    parallelExecution in Test := false
  )

  def examplesSettings = coreSettings ++ mlSettings ++ sparkSettings ++ Seq(
    name := "sblaj-examples"
  )
}
