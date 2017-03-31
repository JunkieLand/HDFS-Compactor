package it.trenzalore.hdfs.compactor.run

import org.apache.spark.sql.{ DataFrame, DataFrameWriter, SaveMode, SparkSession }
import it.trenzalore.hdfs.compactor.formats.CompressionFormat
import com.twitter.conversions.storage._
import com.databricks.spark.avro._

trait FileFormatWriter {

  def sparkFormat: String

  def spark: SparkSession

  def compressionValue(outputCompressionFormat: CompressionFormat): String

  def setWriteConfiguration[T](
    dataFrameWriter:         DataFrameWriter[T],
    outputCompressionFormat: CompressionFormat
  ): DataFrameWriter[T]

  def writeDataFrame(
    dataFrame:               DataFrame,
    outputDirectory:         String,
    saveMode:                SaveMode,
    outputCompressionFormat: CompressionFormat
  ): Unit = {
    setWriteConfiguration(dataFrame.write, outputCompressionFormat)
      .mode(saveMode)
      .format(sparkFormat)
      .save(outputDirectory)
  }

}

case class ParquetWriter(implicit spark: SparkSession) extends FileFormatWriter {

  val sparkFormat = "parquet"

  def compressionValue(outputCompressionFormat: CompressionFormat): String = {
    outputCompressionFormat match {
      case CompressionFormat.GZip   ⇒ "gzip"
      case CompressionFormat.Snappy ⇒ "snappy"
      case _                        ⇒ "none"
    }
  }

  def setWriteConfiguration[T](
    dataFrameWriter:         DataFrameWriter[T],
    outputCompressionFormat: CompressionFormat
  ): DataFrameWriter[T] = {
    setBlockSize()
    dataFrameWriter.option("compression", compressionValue(outputCompressionFormat))
  }

  private def setBlockSize() = {
    val hdfsBlockSize = spark.sparkContext.hadoopConfiguration.getLong("dfs.blocksize", 128.megabytes.inBytes)
    spark.sparkContext.hadoopConfiguration.setLong("parquet.block.size", hdfsBlockSize)
  }

}

case class AvroWriter(implicit spark: SparkSession) extends FileFormatWriter {

  val sparkFormat = "com.databricks.spark.avro"

  def compressionValue(outputCompressionFormat: CompressionFormat): String = {
    outputCompressionFormat match {
      case CompressionFormat.Deflate ⇒ "deflate"
      case CompressionFormat.Snappy  ⇒ "snappy"
      case _                         ⇒ "uncompressed"
    }
  }

  def setWriteConfiguration[T](
    dataFrameWriter:         DataFrameWriter[T],
    outputCompressionFormat: CompressionFormat
  ): DataFrameWriter[T] = {
    spark.conf.set("spark.sql.avro.compression.codec", compressionValue(outputCompressionFormat))
    dataFrameWriter
  }

}

// // Source : https://spark.apache.org/docs/2.0.0/api/java/org/apache/spark/sql/DataFrameWriter.html
// private def getSparkCompression(outputCompressionFormat: CompressionFormat): String = {
//   outputCompressionFormat match {
//     case CompressionFormat.GZip         ⇒ "gzip"
//     case CompressionFormat.Snappy       ⇒ "snappy"
//     case CompressionFormat.LZO          ⇒ "lzo"
//     case CompressionFormat.LZ4          ⇒ "lz4"
//     case CompressionFormat.BZip2        ⇒ "bzip2"
//     case CompressionFormat.Deflate      ⇒ "deflate"
//     case CompressionFormat.ZLib         ⇒ "zlib"
//     case CompressionFormat.Uncompressed ⇒ "none"
//   }
// }