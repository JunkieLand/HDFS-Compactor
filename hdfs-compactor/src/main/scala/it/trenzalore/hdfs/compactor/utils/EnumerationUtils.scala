package it.trenzalore.hdfs.compactor.utils

trait CustomEnumeration[T] {
  def _ALL: Seq[T]

  def fromString(name: String): Option[T] = _ALL.find(_.toString.toLowerCase == name.toLowerCase)
}