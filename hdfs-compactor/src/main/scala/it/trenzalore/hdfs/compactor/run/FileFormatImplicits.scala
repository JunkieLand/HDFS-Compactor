package it.trenzalore.hdfs.compactor.run

import it.trenzalore.hdfs.compactor.formats.FileFormat
import it.trenzalore.hdfs.compactor.formats.FileFormat._
import org.apache.spark.sql.SparkSession

object FileFormatImplicits {

  implicit class EnhencedFileFormat(fileFormat: FileFormat) {
    def getReader(): FileFormatReader = fileFormat match {
      case Parquet ⇒ ParquetReader
      case Avro    ⇒ AvroReader
    }
    def getWriter(implicit spark: SparkSession): FileFormatWriter = fileFormat match {
      case Parquet ⇒ ParquetWriter()
      case Avro    ⇒ AvroWriter()
    }
  }

}