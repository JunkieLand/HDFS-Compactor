package it.trenzalore.build.settings

import sbt.Keys._
import sbt.{ Credentials, Path, _ }

object PublishSettings {

  /** Configuration for publishing artifacts on Nexus.
    */
  lazy val publishSettings = Seq(
    credentials += Credentials(Path.userHome / ".ivy2" / ".nexus-credentials"),
    publishTo := {
      val nexus = "http://localhost:5555/nexus/content/repositories"
      if (version.value.trim.endsWith("SNAPSHOT"))
        Some("snapshots" at nexus + "/snapshots/")
      else
        Some("releases" at nexus + "/releases/")
    },
    publishArtifact in (Compile, packageSrc) := false, // we don't want to publish the source of the packages
    publishArtifact in (Compile, packageDoc) := false, // we don't want to publish the javadoc of the packages
    publishArtifact in (Test, packageSrc) := false
  )

}