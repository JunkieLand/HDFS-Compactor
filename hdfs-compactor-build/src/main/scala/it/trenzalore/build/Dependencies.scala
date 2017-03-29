package it.trenzalore.build

import sbt._

object Versions {
  val scalaVersion = "2.11.8"
  val sbtVersion = "0.13.13"
  val defaultSparkVersion = "2.1.0"
}

object Dependencies {

  import Versions._

  // The repositories where to download packages (jar) from
  val resolvers = Seq(
    "sonatype-oss" at "http://oss.sonatype.org/content/repositories/snapshots",
    "OSS" at "http://oss.sonatype.org/content/repositories/releases",
    "Typesafe Releases" at "http://repo.typesafe.com/typesafe/releases/"
  )

  val commonLibraries = Seq(
    "org.joda" % "joda-convert" % "1.8.1",
    "joda-time" % "joda-time" % "2.9.6",
    "org.scalatest" %% "scalatest" % "2.2.5" % "test",
    "org.scalamock" %% "scalamock-scalatest-support" % "3.2.2" % "test"
  ).map(_.exclude("org.mortbay.jetty", "servlet-api"))

  def sparkDependencies(sparkVersion: String, isProvided: Boolean) = {
    val scope = if (isProvided) "provided" else "compile"

    Seq(
      "org.apache.spark" %% "spark-core" % sparkVersion % scope,
      "org.apache.spark" %% "spark-sql" % sparkVersion % scope,
      "org.apache.spark" %% "spark-streaming" % sparkVersion % scope,
      "org.apache.spark" %% "spark-mllib" % sparkVersion % scope,
      "org.apache.spark" %% "spark-hive" % sparkVersion % scope,
      "org.apache.spark" %% "spark-yarn" % sparkVersion % scope,
      "com.databricks" %% "spark-avro" % "3.2.0"
    )
  }

  val scopt = "com.github.scopt" %% "scopt" % "3.5.0"

  // Note : versions >= 1.3 are compiled with java 8 thus don't work with our cloudera java 7 environment
  val typesafeConfig = "com.typesafe" % "config" % "1.2.1"

  val logback = "ch.qos.logback" % "logback-classic" % "1.1.8"

}