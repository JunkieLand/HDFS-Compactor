package it.trenzalore.hdfs.compactor.run

import org.apache.spark.sql.{ SaveMode, SparkSession, DataFrame, DataFrameWriter }
import it.trenzalore.hdfs.compactor.configuration.CompactionConfiguration
import it.trenzalore.hdfs.compactor.formats.{ CompressionFormat, FileFormat }
import com.twitter.conversions.storage._

object CompactorRunner {

  def run(configuration: CompactionConfiguration)(implicit spark: SparkSession) = {
    val dataFrame = getDataFrame(configuration.inputFileFormat, configuration.inputFiles)

    val dataFrameWriter = dataFrame
      .coalesce(1)
      .write
      .option("compression", getSparkCompression(configuration.outputCompressionFormat))
      .mode(SaveMode.Append)

    writeDataFrame(dataFrameWriter, configuration.outputFileFormat, configuration.outputDirectory)
  }

  def getDataFrame(inputFileFormat: FileFormat, inputFiles: Seq[String])(implicit spark: SparkSession): DataFrame = {
    val dataFrameReader = spark.read

    inputFileFormat match {
      case FileFormat.Parquet ⇒ dataFrameReader.parquet(inputFiles: _*)
    }
  }

  def writeDataFrame[T](
    dataFrameWriter:  DataFrameWriter[T],
    outputFileFormat: FileFormat,
    outputDirectory:  String
  )(implicit spark: SparkSession) = {
    outputFileFormat match {
      case FileFormat.Parquet ⇒ dataFrameWriter.parquet(outputDirectory)
    }
  }

  // Source : https://spark.apache.org/docs/2.0.0/api/java/org/apache/spark/sql/DataFrameWriter.html
  def getSparkCompression(outputCompressionFormat: CompressionFormat): String = {
    outputCompressionFormat match {
      case CompressionFormat.GZip         ⇒ "gzip"
      case CompressionFormat.Snappy       ⇒ "snappy"
      case CompressionFormat.LZO          ⇒ "lzo"
      case CompressionFormat.LZ4          ⇒ "lz4"
      case CompressionFormat.BZip2        ⇒ "bzip2"
      case CompressionFormat.Deflate      ⇒ "deflate"
      case CompressionFormat.ZLib         ⇒ "zlib"
      case CompressionFormat.Uncompressed ⇒ "none"
    }
  }

  def setBlockSize(outputFileFormat: FileFormat)(implicit spark: SparkSession) = {
    val hdfsBlockSize = spark.sparkContext.hadoopConfiguration.getLong("dfs.blocksize", 128.megabytes.inBytes)
    outputFileFormat match {
      case FileFormat.Parquet ⇒ spark.sparkContext.hadoopConfiguration.setLong("parquet.block.size", hdfsBlockSize)
    }
  }

}