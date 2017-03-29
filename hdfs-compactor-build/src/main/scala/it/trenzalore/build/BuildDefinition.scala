package it.trenzalore.build

import it.trenzalore.build.settings.{ ProjectSettings, ReleaseSettings, ScalariformSettings }
import sbt.Keys._
import sbt._

object BuildDefinition {

  import Implicits._

  def sparkApp(
    name:            String,
    publication:     Publication,
    sparkVersion:    String      = Versions.defaultSparkVersion,
    isSparkProvided: Boolean     = true
  ) = {
    basicApp(name, publication)
      .settings(
        libraryDependencies ++= Seq(
          Dependencies.scopt,
          Dependencies.typesafeConfig
        ) ++ Dependencies.sparkDependencies(sparkVersion, isSparkProvided)
      )
  }

  def basicApp(name: String, publication: Publication) = {
    Project(name, new File("."))
      .settings(
        organization := ProjectSettings.organization,
        scalaVersion := Versions.scalaVersion,
        sbtVersion := Versions.sbtVersion,
        crossScalaVersions := Seq(scalaVersion.value),
        logLevel := Level.Info,
        parallelExecution := false,
        offline := true,
        fork := true,
        javaOptions += "-Dsbt.override.build.repos=true -Xmx8G",
        mappings in (Compile, packageBin) ~= ignoreFiles("/data/", "/fic_test_NPJ")
      )
      .settings(ScalariformSettings.scalariformPluginSettings: _*)
      .settings(ReleaseSettings.releaseSettings: _*)
      .settings(
        resolvers ++= Dependencies.resolvers,
        libraryDependencies ++= Dependencies.commonLibraries
      )
      .mapCondition(publication == None, project ⇒ project.isNotPublishable())
      .mapCondition(publication == Assembly, project ⇒ project.publishAssembly())
      .mapCondition(publication == Pack, project ⇒ project.publishPack())
  }

  private def ignoreFiles(ignoredFiles: String*): (Seq[(File, String)]) ⇒ Seq[(File, String)] = { mapping ⇒
    mapping.filter {
      case (file, map) ⇒
        ignoredFiles.forall(ignoredFile ⇒ !file.getAbsolutePath.contains(ignoredFile))
    }
  }

}
