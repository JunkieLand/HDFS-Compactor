package it.trenzalore.hdfs.compactor.formats

import it.trenzalore.hdfs.compactor.utils.CustomEnumeration

sealed trait CompressionFormat

object CompressionFormat extends CustomEnumeration[CompressionFormat] {
  case object GZip extends CompressionFormat
  case object Snappy extends CompressionFormat
  case object LZO extends CompressionFormat
  case object LZ4 extends CompressionFormat
  case object BZip2 extends CompressionFormat
  case object Deflate extends CompressionFormat
  case object ZLib extends CompressionFormat
  case object Uncompressed extends CompressionFormat

  val _ALL = Vector(GZip, Snappy, LZO, LZ4, BZip2, Deflate, ZLib, Uncompressed)
}