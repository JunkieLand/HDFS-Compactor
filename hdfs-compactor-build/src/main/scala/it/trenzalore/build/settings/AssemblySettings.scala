package it.trenzalore.build.settings

import sbt.Keys._
import sbt._
import sbtassembly.AssemblyPlugin.autoImport._
import sbtassembly.PathList

object AssemblySettings {

  /** Configuration of Assembly Plugin : generate a fat jar, meaning a jar with all the librairies inside
    *
    * url : https://github.com/sbt/sbt-assembly
    *
    * command :
    * - sbt assembly
    */
  lazy val assemblySettings = Seq(
    test in assembly := {}, // Do not play test when building assembly
    assemblyOption in assembly := (assemblyOption in assembly).value.copy(cacheOutput = false), // Do not check SHA-1 of each jar/class
    assemblyMergeStrategy in assembly := {
      case PathList("META-INF", xs @ _*) ⇒ MergeStrategy.discard // Do not merge META-INF files
      case x                             ⇒ MergeStrategy.first // take the first class that match the name
    },
    artifact in (Compile, assembly) := {
      val art = (artifact in (Compile, assembly)).value
      art.copy(`classifier` = Some("assembly"))
    },
    packagedArtifacts := Map(), // remove all other packages, we only want to package assembly jar
    addArtifact(artifact in (Compile, assembly), assembly)
  )

}