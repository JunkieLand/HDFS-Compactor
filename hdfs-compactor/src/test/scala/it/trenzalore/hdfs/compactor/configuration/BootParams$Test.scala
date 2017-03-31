package it.trenzalore.hdfs.compactor.configuration

import org.scalatest.{ FunSuite, Matchers }
import it.trenzalore.hdfs.compactor.formats.{ CompressionFormat, FileFormat }

class BootParams$Test extends FunSuite with Matchers {

  val fullCommandLine = "--input-directory dummyDir --input-file-format parquet --input-compression-format snappy --output-directory dummyOutDir --output-file-format Parquet --output-compression-format GZip --delete-input-files true"

  def commandLineWithout(param: String): String = {
    val regex = s"--${param} \\w.+"
    fullCommandLine.replaceAll(regex, "")
  }

  def commandLineWithTwice(param: String, value: String): String = fullCommandLine + s"--${param} $value"

  test("All parameters can be provided") {
    // -- Given
    val params = fullCommandLine.split(" ")

    // -- When
    val bootParams = BootParams.parse(params).get

    // -- Then
    bootParams.inputDirectory should be("dummyDir")
    bootParams.inputFileFormat should be(FileFormat.Parquet)
    bootParams.inputCompressionFormat should be(CompressionFormat.Snappy)
    bootParams.outputDirectory should be("dummyOutDir")
    bootParams.outputFileFormat should be(FileFormat.Parquet)
    bootParams.outputCompressionFormat should be(CompressionFormat.GZip)
    bootParams.deleteInputFiles should be(true)
  }

  test("input-file-format parameter should at least once") {
    // -- Given
    val params = commandLineWithout("input-file-format").split(" ")

    // -- When
    val bootParams = BootParams.parse(params)

    // -- Then
    bootParams should be(None)
  }

  test("input-file-format parameter should exist maximum once") {
    // -- Given
    val params = commandLineWithTwice("input-file-format", "parquet").split(" ")

    // -- When
    val bootParams = BootParams.parse(params)

    // -- Then
    bootParams should be(None)
  }

  test("input-compression-format parameter should at least once") {
    // -- Given
    val params = commandLineWithout("input-compression-format").split(" ")

    // -- When
    val bootParams = BootParams.parse(params)

    // -- Then
    bootParams should be(None)
  }

  test("input-compression-format parameter should exist maximum once") {
    // -- Given
    val params = commandLineWithTwice("input-compression-format", "parquet").split(" ")

    // -- When
    val bootParams = BootParams.parse(params)

    // -- Then
    bootParams should be(None)
  }

  test("output-directory parameter should at least once") {
    // -- Given
    val params = commandLineWithout("output-directory").split(" ")

    // -- When
    val bootParams = BootParams.parse(params)

    // -- Then
    bootParams should be(None)
  }

  test("output-directory parameter should exist maximum once") {
    // -- Given
    val params = commandLineWithTwice("output-directory", "parquet").split(" ")

    // -- When
    val bootParams = BootParams.parse(params)

    // -- Then
    bootParams should be(None)
  }

  test("output-file-format parameter should at least once") {
    // -- Given
    val params = commandLineWithout("output-file-format").split(" ")

    // -- When
    val bootParams = BootParams.parse(params)

    // -- Then
    bootParams should be(None)
  }

  test("output-file-format parameter should exist maximum once") {
    // -- Given
    val params = commandLineWithTwice("output-file-format", "parquet").split(" ")

    // -- When
    val bootParams = BootParams.parse(params)

    // -- Then
    bootParams should be(None)
  }

  test("output-compression-format parameter should at least once") {
    // -- Given
    val params = commandLineWithout("output-compression-format").split(" ")

    // -- When
    val bootParams = BootParams.parse(params)

    // -- Then
    bootParams should be(None)
  }

  test("output-compression-format parameter should exist maximum once") {
    // -- Given
    val params = commandLineWithTwice("output-compression-format", "parquet").split(" ")

    // -- When
    val bootParams = BootParams.parse(params)

    // -- Then
    bootParams should be(None)
  }

  test("delete-input-files parameter should be optionnal") {
    // -- Given
    val params = commandLineWithout("delete-input-files").split(" ")

    // -- When
    val bootParams = BootParams.parse(params).get

    // -- Then
    bootParams.inputDirectory should be("dummyDir")
    bootParams.inputFileFormat should be(FileFormat.Parquet)
    bootParams.inputCompressionFormat should be(CompressionFormat.Snappy)
    bootParams.outputDirectory should be("dummyOutDir")
    bootParams.outputFileFormat should be(FileFormat.Parquet)
    bootParams.outputCompressionFormat should be(CompressionFormat.GZip)
    bootParams.deleteInputFiles should be(false)
  }

  test("delete-input-files parameter should exist maximum once") {
    // -- Given
    val params = commandLineWithTwice("delete-input-files", "parquet").split(" ")

    // -- When
    val bootParams = BootParams.parse(params)

    // -- Then
    bootParams should be(None)
  }

}