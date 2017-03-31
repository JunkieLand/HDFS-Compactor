package it.trenzalore.hdfs.compactor.run

import it.trenzalore.hdfs.compactor.formats.FileFormat
import org.apache.spark.sql.{ DataFrame, SparkSession }

trait FileFormatReader {
  def getDataFrame(inputFiles: Seq[String])(implicit spark: SparkSession): DataFrame
}

object ParquetReader extends FileFormatReader {
  def getDataFrame(inputFiles: Seq[String])(implicit spark: SparkSession): DataFrame = {
    spark.read.parquet(inputFiles: _*)
  }
}

object AvroReader extends FileFormatReader {
  def getDataFrame(inputFiles: Seq[String])(implicit spark: SparkSession): DataFrame = {
    spark.read.format("com.databricks.spark.avro").load(inputFiles: _*)
  }
}