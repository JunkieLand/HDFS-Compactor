package it.trenzalore.hdfs.compactor.configuration

import org.scalatest.{ FunSuite, Matchers, BeforeAndAfter }
import it.trenzalore.hdfs.compactor.formats.{ CompressionFormat, FileFormat }
import it.trenzalore.hdfs.compactor.testutils.TestSparkEnv
import org.apache.hadoop.fs.FileSystem

class CompactionConfiguration$Test extends FunSuite with Matchers with BeforeAndAfter {

  implicit val fs = TestSparkEnv.fs

  test("Should list the input files") {
    // -- Given
    val inputDirectory = "src/test/resources/dummySnappyParquetFixtures"

    // -- When
    val inputFiles = CompactionConfiguration.getInputFiles(inputDirectory)

    // -- Then
    inputFiles.size should be(3)
    inputFiles.find(_.endsWith("part-00000-fc6d5dd6-0f84-4db7-b9cb-98f72c2e9cbf.snappy.parquet")).get should startWith("file:")
    inputFiles.find(_.endsWith("part-00001-fc6d5dd6-0f84-4db7-b9cb-98f72c2e9cbf.snappy.parquet")).get should startWith("file:")
    inputFiles.find(_.endsWith("part-00002-fc6d5dd6-0f84-4db7-b9cb-98f72c2e9cbf.snappy.parquet")).get should startWith("file:")
  }

  test("Should find parquet file format in input files") {
    // -- Given
    val inputFiles = Seq(
      "part-r-00000-2df65123-0fb2-402a-914a-91cd2960f042.snappy.parquet",
      "part-r-00001-2df65123-0fb2-402a-914a-91cd2960f042.snappy.parquet"
    )
    val userInputFileFormat = None

    // -- When
    val inputFileFormat = CompactionConfiguration.getInputFileFormat(inputFiles, userInputFileFormat).get

    // -- Then
    inputFileFormat should be(FileFormat.Parquet)
  }

  test("Should not find file format in input files") {
    // -- Given
    val inputFiles = Seq(
      "undeterminated-format-file-00.dummy",
      "undeterminated-format-file-01.dummy"
    )
    val userInputFileFormat = None

    // -- When
    val inputFileFormat = CompactionConfiguration.getInputFileFormat(inputFiles, userInputFileFormat)

    // -- Then
    inputFileFormat should be(None)
  }

  test("Should use user provided output file format") {
    // -- Given
    val inputFileFormat = Some(FileFormat.Parquet)
    val userInputFileFormat = Some(FileFormat.Avro)

    // -- When
    val outputFileFormat = CompactionConfiguration.getOutputFileFormat(inputFileFormat, userInputFileFormat).get

    // -- Then
    outputFileFormat should be(FileFormat.Avro)
  }

  test("Should use input file format for output if no provided by user") {
    // -- Given
    val inputFileFormat = Some(FileFormat.Parquet)
    val userOutputFileFormat = None

    // -- When
    val outputFileFormat = CompactionConfiguration.getOutputFileFormat(inputFileFormat, userOutputFileFormat).get

    // -- Then
    outputFileFormat should be(FileFormat.Parquet)
  }

  test("Should have no output file format") {
    // -- Given
    val inputFileFormat = None
    val userOutputFileFormat = None

    // -- When
    val outputFileFormat = CompactionConfiguration.getOutputFileFormat(inputFileFormat, userOutputFileFormat)

    // -- Then
    outputFileFormat should be(None)
  }

  test("Should find snappy file compression format in input files") {
    // -- Given
    val inputFiles = Seq(
      "part-r-00000-2df65123-0fb2-402a-914a-91cd2960f042.snappy.parquet",
      "part-r-00001-2df65123-0fb2-402a-914a-91cd2960f042.snappy.parquet"
    )

    // -- When
    val inputFileCompressionFormat = CompactionConfiguration.getInputCompressionFormat(inputFiles).get

    // -- Then
    inputFileCompressionFormat should be(CompressionFormat.Snappy)
  }

  test("Should not find file compression format in input files") {
    // -- Given
    val inputFiles = Seq(
      "undeterminated-compression-format-file-00.dummy",
      "undeterminated-compression-format-file-01.dummy"
    )

    // -- When
    val inputFileCompressionFormat = CompactionConfiguration.getInputCompressionFormat(inputFiles)

    // -- Then
    inputFileCompressionFormat should be(None)
  }

  test("Should use user provided output file compression format") {
    // -- Given
    val inputFileCompressionFormat = Some(CompressionFormat.Snappy)
    val userOutputFileCompressionFormat = Some(CompressionFormat.GZip)
    val outputFileFormat = FileFormat.Parquet

    // -- When
    val outputFileCompressionFormat = CompactionConfiguration
      .getOuputCompressionFormat(inputFileCompressionFormat, userOutputFileCompressionFormat, outputFileFormat)

    // -- Then
    outputFileCompressionFormat should be(CompressionFormat.GZip)
  }

  test("Should use input file compression format if no provided by user") {
    // -- Given
    val inputFileCompressionFormat = Some(CompressionFormat.Snappy)
    val userOutputFileCompressionFormat = None
    val outputFileFormat = FileFormat.Parquet

    // -- When
    val outputFileCompressionFormat = CompactionConfiguration
      .getOuputCompressionFormat(inputFileCompressionFormat, userOutputFileCompressionFormat, outputFileFormat)

    // -- Then
    outputFileCompressionFormat should be(CompressionFormat.Snappy)
  }

  test("Should use default compression format for output file format") {
    // -- Given
    val inputFileCompressionFormat = None
    val userOutputFileCompressionFormat = None
    val outputFileFormat = FileFormat.Parquet

    // -- When
    val outputFileCompressionFormat = CompactionConfiguration
      .getOuputCompressionFormat(inputFileCompressionFormat, userOutputFileCompressionFormat, outputFileFormat)

    // -- Then
    outputFileCompressionFormat should be(CompressionFormat.Snappy)
  }

  test("Output compression format should raise an error if incompatible with output file format") {
    // -- Given
    val compactionConfiguration = CompactionConfiguration(
      inputFiles = Seq(),
      inputFileFormat = FileFormat.Parquet,
      outputDirectory = "",
      outputFileFormat = FileFormat.Parquet,
      outputCompressionFormat = CompressionFormat.LZO
    )

    // -- When
    val e = intercept[IllegalArgumentException] {
      CompactionConfiguration.checkConfiguration(compactionConfiguration)
    }

    // -- Then
    e.getMessage should include("Compression format LZO is incompatible with file format Parquet. Select one in : Vector(Snappy, GZip, Uncompressed)")
  }

  test("Output compression format should be compatible with output file format") {
    // -- Given
    val compactionConfiguration = CompactionConfiguration(
      inputFiles = Seq(""),
      inputFileFormat = FileFormat.Parquet,
      outputDirectory = "",
      outputFileFormat = FileFormat.Parquet,
      outputCompressionFormat = CompressionFormat.Snappy
    )

    // -- When
    CompactionConfiguration.checkConfiguration(compactionConfiguration)
  }

  test("Input files should not be empty") {
    // -- Given
    object CompactionConfigurationMock extends CompactionConfigurationTrait {
      override def getInputFiles(inputDirectory: String)(implicit fs: FileSystem): Seq[String] = Nil
    }

    val bootParams = BootParams()

    // -- When
    val e = intercept[IllegalArgumentException] {
      CompactionConfigurationMock.compute(bootParams)
    }

    // -- Then
    e.getMessage should include("You should provide input files or an input directory in argument")
  }

  test("Input file format should not be empty") {
    // -- Given
    object CompactionConfigurationMock extends CompactionConfigurationTrait {
      override def getInputFiles(inputDirectory: String)(implicit fs: FileSystem): Seq[String] = Seq("dummyInput")

      override def getInputFileFormat(inputFiles: Seq[String], userInputFileFormat: Option[FileFormat]): Option[FileFormat] = None
    }

    val bootParams = BootParams()

    // -- When
    val e = intercept[IllegalArgumentException] {
      CompactionConfigurationMock.compute(bootParams)
    }

    // -- Then
    e.getMessage should include("You should provide an input file format in argument")
  }

  test("Output file format should not be empty") {
    // -- Given
    object CompactionConfigurationMock extends CompactionConfigurationTrait {
      override def getInputFiles(inputDirectory: String)(implicit fs: FileSystem): Seq[String] = Seq("dummyInput")

      override def getInputFileFormat(inputFiles: Seq[String], userInputFileFormat: Option[FileFormat]): Option[FileFormat] = Some(FileFormat.Parquet)

      override def getOutputFileFormat(
        inputFileFormat:      Option[FileFormat],
        userOutputFileFormat: Option[FileFormat]
      ): Option[FileFormat] = None
    }

    val bootParams = BootParams()

    // -- When
    val e = intercept[IllegalArgumentException] {
      CompactionConfigurationMock.compute(bootParams)
    }

    // -- Then
    e.getMessage should include("You should provide an output file format in argument")
  }

}