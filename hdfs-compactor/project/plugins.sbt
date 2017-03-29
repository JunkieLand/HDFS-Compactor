
lazy val hdfsCompactorBuild = file("../../hdfs-compactor-build")

lazy val root = project
  .in(file("."))
  .dependsOn(hdfsCompactorBuild)