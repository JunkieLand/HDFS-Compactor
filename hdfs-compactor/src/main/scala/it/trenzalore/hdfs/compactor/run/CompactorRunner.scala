package it.trenzalore.hdfs.compactor.run

import org.apache.spark.sql.{ SaveMode, SparkSession, DataFrame, DataFrameWriter }
import it.trenzalore.hdfs.compactor.configuration.BootParams
import it.trenzalore.hdfs.compactor.formats.{ CompressionFormat, FileFormat }
import com.twitter.conversions.storage._
import com.databricks.spark.avro._
import org.apache.hadoop.fs.{ FileSystem, Path, PathFilter }
import org.slf4j.LoggerFactory

object CompactorRunner {

  import FileFormatImplicits._

  lazy val logger = LoggerFactory.getLogger(getClass())

  def run(params: BootParams)(implicit spark: SparkSession, fs: FileSystem) = {
    val inputFiles = getInputFiles(params.inputDirectory)

    val blockSize = spark.sparkContext.hadoopConfiguration.getLong("dfs.blocksize", 128.megabytes.inBytes)

    val outputSizeOpt = estimateOutputSize(params)

    val nbOfFiles = outputSizeOpt
      .map(outputSize ⇒ Math.ceil(outputSize.toDouble / blockSize.toDouble))
      .map(_.toInt)
      .getOrElse(1)

    val dataFrame = params
      .inputFileFormat
      .getReader
      .getDataFrame(inputFiles)
      .coalesce(nbOfFiles)

    params
      .outputFileFormat
      .getWriter
      .writeDataFrame(
        dataFrame = dataFrame,
        outputDirectory = params.outputDirectory,
        saveMode = SaveMode.Append,
        outputCompressionFormat = params.outputCompressionFormat
      )

    if (params.deleteInputFiles) {
      deleteInputFiles(inputFiles)
    }
  }

  def deleteInputFiles(inputFiles: Seq[String])(implicit fs: FileSystem) = {
    logger.info("Will delete {} uncompacted input files", inputFiles.size)
    inputFiles.foreach(inputFile ⇒ fs.delete(new Path(inputFile), false))
  }

  // If input and output have the same file format and the same compression format,
  // we estimate that they will have roughly the same size.
  def estimateOutputSize(params: BootParams)(implicit fs: FileSystem): Option[Long] = {
    if (params.inputFileFormat == params.outputFileFormat && params.inputCompressionFormat == params.outputCompressionFormat) {
      val inputLength = fs.getContentSummary(new Path(params.inputDirectory)).getLength
      Some(inputLength)
    } else {
      None
    }
  }

  def getInputFiles(inputDirectory: String)(implicit fs: FileSystem): Seq[String] = {
    logger.info("Scanning directory {} for files to compact", inputDirectory)

    fs
      .listStatus(new Path(inputDirectory), new PathFilter {
        def accept(path: Path) = !path.getName.contains("_SUCCESS")
      })
      .toVector
      .map(_.getPath.toString)
  }

}
