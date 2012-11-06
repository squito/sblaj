import sbt._
import Keys._

object SparkBuild extends Build {
  lazy val core = Project("core", file("core"), settings = coreSettings)

  lazy val ml = Project("ml", file("ml"), settings = mlSettings) dependsOn (core)
  lazy val spark = Project("spark", file("spark"), settings = sparkSettings) dependsOn (core)

  def sharedSettings = Defaults.defaultSettings ++ Seq(
    organization := "org.sblaj",
    version := "0.1",
    scalaVersion := "2.9.1",
    scalacOptions := Seq(/*"-deprecation",*/ "-unchecked", "-optimize"), // -deprecation is too noisy due to usage of old Hadoop API, enable it once that's no longer an issue
    unmanagedJars in Compile <<= baseDirectory map { base => (base / "lib" ** "*.jar").classpath },
    retrieveManaged := true,
    transitiveClassifiers in Scope.GlobalScope := Seq("sources"),
    publishTo <<= baseDirectory { base => Some(Resolver.file("Local", base / "target" / "maven" asFile)(Patterns(true, Resolver.mavenStyleBasePattern))) },
    libraryDependencies ++= Seq(
      "org.eclipse.jetty" % "jetty-server" % "7.5.3.v20111011",
      "org.scalatest" %% "scalatest" % "1.6.1" % "test",
      "org.scalacheck" %% "scalacheck" % "1.9" % "test"
    )
  )

  val slf4jVersion = "1.6.1"

  def coreSettings = sharedSettings ++ Seq(
    name := "sblaj-core",
    resolvers ++= Seq(
      "Typesafe Repository" at "http://repo.typesafe.com/typesafe/releases/",
      "JBoss Repository" at "http://repository.jboss.org/nexus/content/repositories/releases/"
    ),
    libraryDependencies ++= Seq(
      "log4j" % "log4j" % "1.2.16",
      "org.slf4j" % "slf4j-api" % slf4jVersion,
      "org.slf4j" % "slf4j-log4j12" % slf4jVersion
    )
  )

  def mlSettings = sharedSettings ++ Seq(
    name := "sblaj-ml"
  )

  def sparkSettings = sharedSettings ++ Seq(
    name := "sblaj-spark",
    libraryDependencies ++= Seq(
      "org.spark-project" % "spark-core_2.9.2" % "0.6.0"
    )
  )
}
