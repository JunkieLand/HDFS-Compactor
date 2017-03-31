package it.trenzalore.hdfs.compactor.configuration

import org.apache.hadoop.fs.{ FileSystem, Path, PathFilter }
import it.trenzalore.hdfs.compactor.formats.{ CompressionFormat, FileFormat }
import org.slf4j.LoggerFactory

case class CompactionConfiguration(
  inputFiles:              Seq[String],
  inputFileFormat:         FileFormat,
  outputDirectory:         String,
  outputFileFormat:        FileFormat,
  outputCompressionFormat: CompressionFormat,
  deleteInputFiles:        Boolean
)

trait CompactionConfigurationTrait {

  lazy val logger = LoggerFactory.getLogger(getClass())

  def compute(bootParams: BootParams)(implicit fs: FileSystem): CompactionConfiguration = {
    val inputFiles = getInputFiles(bootParams.inputDirectory)
    require(inputFiles.nonEmpty, "You should provide input files or an input directory in argument")

    val inputFileFormat = getInputFileFormat(inputFiles, bootParams.inputFileFormat)
    require(inputFileFormat.nonEmpty, "You should provide an input file format in argument")

    val outputFileFormat = getOutputFileFormat(inputFileFormat, bootParams.outputFileFormat)
    require(outputFileFormat.isDefined, "You should provide an output file format in argument")

    val outputCompressionFormat = getOuputCompressionFormat(
      getInputCompressionFormat(inputFiles),
      bootParams.outputCompressionFormat,
      outputFileFormat.get
    )

    CompactionConfiguration(
      inputFiles = inputFiles,
      inputFileFormat = inputFileFormat.get,
      outputDirectory = getOutputDirectory(bootParams.inputDirectory, bootParams.outputDirectory),
      outputFileFormat = outputFileFormat.get,
      outputCompressionFormat = outputCompressionFormat,
      deleteInputFiles = bootParams.deleteInputFiles
    )
  }

  def getInputFiles(inputDirectory: String)(implicit fs: FileSystem): Seq[String] = {
    logger.info("Scanning directory {} for files to compact", inputDirectory)

    fs
      .listStatus(new Path(inputDirectory), new PathFilter {
        def accept(path: Path) = !path.getName.contains("_SUCCESS")
      })
      .map(_.getPath.toString)
      .toVector
  }

  def getInputFileFormat(inputFiles: Seq[String], userInputFileFormat: Option[FileFormat]): Option[FileFormat] = {
    userInputFileFormat
      .orElse {
        logger.info("User did not provide any input file format. Will try to guess one according to the file names.")

        if (inputFiles.forall(_.endsWith("parquet"))) {
          logger.info("Found file format of input files to be : {}", FileFormat.Parquet)
          Some(FileFormat.Parquet)
        } else {
          logger.error("Unable to determine file format of input files")
          None
        }
      }
  }

  def getInputCompressionFormat(inputFiles: Seq[String]): Option[CompressionFormat] = {
    if (inputFiles.forall(_.contains("snappy"))) {
      logger.info("Found compression format of input files to be : {}", CompressionFormat.Snappy)
      Some(CompressionFormat.Snappy)
    } else {
      logger.warn("Unable to determine compression format of input files. Will fallback on the user provided one.")
      None
    }
  }

  def getOutputDirectory(inputDirectory: String, userOutputDirectory: Option[String]): String = {
    userOutputDirectory
      .getOrElse {
        logger.info("User did not provide any output directory. Will fallback on the input one : {}", inputDirectory)
        inputDirectory
      }
  }

  def getOutputFileFormat(
    inputFileFormat:      Option[FileFormat],
    userOutputFileFormat: Option[FileFormat]
  ): Option[FileFormat] = {
    userOutputFileFormat.orElse(inputFileFormat) match {
      case ff @ Some(outputFileFormat) ⇒
        logger.info("Will use file format {} for output files", outputFileFormat)
        ff
      case _ ⇒
        logger.error("Could not determine file format for output files. You should provide one in argument.")
        None
    }
  }

  def getOuputCompressionFormat(
    inputCompressionFormat:      Option[CompressionFormat],
    userOutputCompressionFormat: Option[CompressionFormat],
    outputFileFormat:            FileFormat
  ): CompressionFormat = {
    userOutputCompressionFormat
      .map { outputCompressionFormat ⇒
        logger.info("Will use user provided output compression format : {}", outputCompressionFormat)
        outputCompressionFormat
      }
      .orElse {
        logger.info("User did not provide any output compression format. Will fallback on the input files one.")
        inputCompressionFormat
      }
      .getOrElse {
        val defaultCompressionFormat = outputFileFormat.defaultCompressionFormat
        logger.info("No compression format has been found on the input files. Will fallback on the output format default one : {}", defaultCompressionFormat)
        defaultCompressionFormat
      }
  }

  def checkConfiguration(conf: CompactionConfiguration) = {
    val compressionFormat = conf.outputCompressionFormat
    val fileFormat = conf.outputFileFormat
    val compressionFormatIsCompatibleWithFileFormat = fileFormat
      .acceptedCompressionFormat
      .contains(conf.outputCompressionFormat)
    require(compressionFormatIsCompatibleWithFileFormat, s"Compression format $compressionFormat is incompatible with file format Parquet. Select one in : ${fileFormat.acceptedCompressionFormat}")
  }

}

object CompactionConfiguration extends CompactionConfigurationTrait