import com.typesafe.sbt.SbtScalariform.ScalariformKeys

import scalariform.formatter.preferences._

sbtPlugin := true

name := "hdfs-compactor-build"

organization := "it.trenzalore"

addSbtPlugin("com.eed3si9n" % "sbt-buildinfo" % "0.5.0")

addSbtPlugin("com.typesafe.sbt" % "sbt-git" % "0.8.4")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.3")

addSbtPlugin("net.virtual-void" % "sbt-dependency-graph" % "0.8.2")

addSbtPlugin("org.scalariform" % "sbt-scalariform" % "1.6.0")

addSbtPlugin("com.github.gseitz" % "sbt-release" % "1.0.4")

addSbtPlugin("org.scoverage" % "sbt-scoverage" % "1.5.0")

addSbtPlugin("io.get-coursier" % "sbt-coursier" % "1.0.0-M15")

addSbtPlugin("org.xerial.sbt" % "sbt-pack" % "0.8.2")

scalariformSettings ++ Seq(
  ScalariformKeys.preferences in ThisBuild := ScalariformKeys.preferences.value
    .setPreference(RewriteArrowSymbols, true)
    .setPreference(IndentSpaces, 2)
    .setPreference(SpaceBeforeColon, false)
    .setPreference(CompactStringConcatenation, false)
    .setPreference(PreserveSpaceBeforeArguments, false)
    .setPreference(AlignParameters, true)
    .setPreference(AlignArguments, false)
    .setPreference(DoubleIndentClassDeclaration, false)
    .setPreference(FormatXml, true)
    .setPreference(IndentPackageBlocks, true)
    .setPreference(AlignSingleLineCaseStatements, true)
    .setPreference(IndentLocalDefs, false)
    .setPreference(DanglingCloseParenthesis, Force)
    .setPreference(SpaceInsideParentheses, false)
    .setPreference(SpaceInsideBrackets, false)
    .setPreference(SpacesWithinPatternBinders, true)
    .setPreference(MultilineScaladocCommentsStartOnFirstLine, true)
    .setPreference(IndentWithTabs, false)
    .setPreference(CompactControlReadability, false)
    .setPreference(PlaceScaladocAsterisksBeneathSecondAsterisk, true)
    .setPreference(SpacesAroundMultiImports, true)
)

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % "1.7.22" // Work around to fix Jenkins build
)