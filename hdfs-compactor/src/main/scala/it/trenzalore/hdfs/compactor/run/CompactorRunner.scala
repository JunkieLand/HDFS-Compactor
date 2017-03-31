package it.trenzalore.hdfs.compactor.run

import org.apache.spark.sql.{ SaveMode, SparkSession, DataFrame, DataFrameWriter }
import it.trenzalore.hdfs.compactor.configuration.CompactionConfiguration
import it.trenzalore.hdfs.compactor.formats.{ CompressionFormat, FileFormat }
import com.twitter.conversions.storage._
import com.databricks.spark.avro._
import org.apache.hadoop.fs.{ FileSystem, Path }
import org.slf4j.LoggerFactory

object CompactorRunner {

  import FileFormatImplicits._

  lazy val logger = LoggerFactory.getLogger(getClass())

  def run(configuration: CompactionConfiguration)(implicit spark: SparkSession, fs: FileSystem) = {
    val dataFrame = configuration
      .inputFileFormat
      .getReader
      .getDataFrame(configuration.inputFiles)
      .coalesce(1)

    configuration
      .outputFileFormat
      .getWriter
      .writeDataFrame(
        dataFrame = dataFrame,
        outputDirectory = configuration.outputDirectory,
        saveMode = SaveMode.Append,
        outputCompressionFormat = configuration.outputCompressionFormat
      )

    if (configuration.deleteInputFiles) {
      deleteInputFiles(configuration.inputFiles)
    }
  }

  def deleteInputFiles(inputFiles: Seq[String])(implicit fs: FileSystem) = {
    logger.info("Will delete {} uncompacted input files", inputFiles.size)
    inputFiles.foreach(inputFile ⇒ fs.delete(new Path(inputFile), false))
  }

}
