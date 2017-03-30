package it.trenzalore.hdfs.compactor.formats

import it.trenzalore.hdfs.compactor.utils.CustomEnumeration

sealed trait CompressionFormat

object CompressionFormat extends CustomEnumeration[CompressionFormat] {
  case object GZip extends CompressionFormat
  case object Snappy extends CompressionFormat
  case object LZO extends CompressionFormat
  case object Uncompressed extends CompressionFormat

  val _ALL = Vector(GZip, Snappy, LZO, Uncompressed)
}