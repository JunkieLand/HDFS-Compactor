package it.trenzalore.hdfs.compactor.configuration

import it.trenzalore.hdfs.compactor.formats.{ CompressionFormat, FileFormat }

case class BootParams(
  inputDirectory:          String                    = "",
  inputFileFormat:         Option[FileFormat]        = None,
  outputDirectory:         Option[String]            = None,
  outputFileFormat:        Option[FileFormat]        = None,
  outputCompressionFormat: Option[CompressionFormat] = None
)

object BootParams {
  import scopt._

  def parse(args: Array[String]): Option[BootParams] = parser.parse(args, BootParams())

  val parser = new OptionParser[BootParams]("hdfs-compactor") {
    help("help").text("prints this usage text")

    opt[String]('i', "input-directory")
      .minOccurs(1)
      .maxOccurs(1)
      .text("The input directory of which the files must be concatenated.")
      .action {
        case (inputDirectory, bootParams) ⇒
          bootParams.copy(inputDirectory = inputDirectory)
      }

    opt[String]('j', "input-file-format")
      .maxOccurs(1)
      .text("The file format of the input files. If not provided, will try to guess according to file names.")
      .validate { inputFileFormat ⇒
        if (FileFormat.fromString(inputFileFormat).isDefined) success
        else failure(s"input-file-format should be in : ${FileFormat._ALL.toString}")
      }
      .action {
        case (inputFileFormat, bootParams) ⇒
          bootParams.copy(inputFileFormat = FileFormat.fromString(inputFileFormat))
      }

    opt[String]('o', "output-directory")
      .maxOccurs(1)
      .text("The output directory for the concatenated files. Defaults depends on the file format.")
      .action {
        case (outputDirectory, bootParams) ⇒
          bootParams.copy(outputDirectory = Some(outputDirectory))
      }

    opt[String]('f', "output-file-format")
      .maxOccurs(1)
      .text("The output file format. Possible values : parquet, avro, text. Defaults to the same as input.")
      .validate { outputFileFormat ⇒
        if (FileFormat.fromString(outputFileFormat).isDefined) success
        else failure(s"output-file-format should be in : ${FileFormat._ALL.toString}")
      }
      .action {
        case (outputFileFormat, bootParams) ⇒
          bootParams.copy(outputFileFormat = FileFormat.fromString(outputFileFormat))
      }

    opt[String]('c', "output-compression-format")
      .maxOccurs(1)
      .text("The output compression format. Possible values : gzip, snappy, lzo, none. Defaults to the same as input.")
      .validate { outputCompressionFormat ⇒
        if (CompressionFormat.fromString(outputCompressionFormat).isDefined) success
        else failure(s"output-compression-format should be in : ${CompressionFormat._ALL.toString}")
      }
      .action {
        case (outputCompressionFormat, bootParams) ⇒
          bootParams.copy(outputCompressionFormat = CompressionFormat.fromString(outputCompressionFormat))
      }
  }

}