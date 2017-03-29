package it.trenzalore.build.settings

import java.io.File

import sbt.Keys._
import xerial.sbt.Pack._

object PackSettings {

  lazy val packPluginSettings = packSettings ++ Seq(
    packGenerateWindowsBatFile := false,
    packResourceDir ++= Map(new File(baseDirectory.value, "src/main/resources") → "conf"),
    packArchiveStem := s"${packArchivePrefix.value}" // name of the directory inside the archive
  ) ++ publishPackArchiveTgz

}
