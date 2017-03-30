package it.trenzalore.hdfs.compactor.formats

import it.trenzalore.hdfs.compactor.utils.CustomEnumeration

sealed trait FileFormat {
  def acceptedCompressionFormat: Seq[CompressionFormat]
  def defaultCompressionFormat: CompressionFormat
}

object FileFormat extends CustomEnumeration[FileFormat] {
  case object Parquet extends FileFormat {
    val acceptedCompressionFormat = Vector(
      CompressionFormat.Snappy,
      CompressionFormat.GZip,
      CompressionFormat.Uncompressed
    )
    val defaultCompressionFormat = CompressionFormat.Snappy
  }
  case object Avro extends FileFormat {
    val acceptedCompressionFormat = Vector(
      CompressionFormat.Snappy,
      CompressionFormat.GZip,
      CompressionFormat.Uncompressed
    )
    val defaultCompressionFormat = CompressionFormat.Snappy
  }
  case object Text extends FileFormat {
    val acceptedCompressionFormat = Vector(
      CompressionFormat.Snappy,
      CompressionFormat.GZip,
      CompressionFormat.LZO,
      CompressionFormat.Uncompressed
    )
    val defaultCompressionFormat = CompressionFormat.GZip
  }

  val _ALL = Vector(Parquet, Avro, Text)
}