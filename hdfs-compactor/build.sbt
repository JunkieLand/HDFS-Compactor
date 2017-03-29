import it.trenzalore.build.{BuildDefinition, Pack, Versions}


lazy val hdfsCompactor = BuildDefinition
  .sparkApp("hdfs-compactor", Pack, sparkVersion = Versions.defaultSparkVersion, isSparkProvided = false)
  .settings(libraryDependencies ++= Seq(
    "com.twitter" %% "util-collection" % "6.42.0"
  ))
