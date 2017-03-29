package it.trenzalore.build.settings

import com.typesafe.sbt.SbtScalariform.{ ScalariformKeys, _ }
import sbt.ThisBuild

import scalariform.formatter.preferences._

object ScalariformSettings {

  /** Configuration of Scalariform plugin (source code formatting)
    *
    * url : https://github.com/sbt/sbt-scalariform
    */
  lazy val scalariformPluginSettings = scalariformSettings ++ Seq(
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

}