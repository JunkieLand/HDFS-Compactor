package it.trenzalore.hdfs.compactor.configuration

import org.scalatest.{ FunSuite, Matchers }
import it.trenzalore.hdfs.compactor.formats.{ CompressionFormat, FileFormat }

class BootParams$Test extends FunSuite with Matchers {

  test("All parameters can be provided") {
    // -- Given
    val params = "--input-directory dummyDir --input-file-format parquet --output-directory dummyOutDir --output-file-format Parquet --output-compression-format LZO --delete-input-files true".split(" ")

    // -- When
    val bootParams = BootParams.parse(params).get

    // -- Then
    bootParams.inputDirectory should be("dummyDir")
    bootParams.inputFileFormat.get should be(FileFormat.Parquet)
    bootParams.outputDirectory.get should be("dummyOutDir")
    bootParams.outputFileFormat.get should be(FileFormat.Parquet)
    bootParams.outputCompressionFormat.get should be(CompressionFormat.LZO)
    bootParams.deleteInputFiles should be(true)
  }

  test("input-file-format parameter should exist maximum once") {
    // -- Given
    val params = "--input-directory dummyInDir --input-file-format parquet --input-file-format avro".split(" ")

    // -- When
    val bootParams = BootParams.parse(params)

    // -- Then
    bootParams should be(None)
  }

  test("output-directory, output-file-format and output-compression-format can be optionnal") {
    // -- Given
    val params = "--input-directory dummyDir".split(" ")

    // -- When
    val bootParams = BootParams.parse(params).get

    // -- Then
    bootParams.inputDirectory should be("dummyDir")
    bootParams.outputDirectory should be(None)
    bootParams.outputFileFormat should be(None)
    bootParams.outputCompressionFormat should be(None)
  }

  test("input-directory parameter should exist at least once") {
    // -- Given
    val params = "".split(" ")

    // -- When
    val bootParams = BootParams.parse(params)

    // -- Then
    bootParams should be(None)
  }

  test("input-directory parameter should exist maximum once") {
    // -- Given
    val params = "--input-directory dummyDir1 --input-directory dummyDir2".split(" ")

    // -- When
    val bootParams = BootParams.parse(params)

    // -- Then
    bootParams should be(None)
  }

  test("output-directory parameter should exist maximum once") {
    // -- Given
    val params = "--input-directory dummyInDir --output-directory dummyOutDir1 --output-directory dummyOutDir2".split(" ")

    // -- When
    val bootParams = BootParams.parse(params)

    // -- Then
    bootParams should be(None)
  }

  test("output-file-format parameter should exist maximum once") {
    // -- Given
    val params = "--input-directory dummyInDir --output-file-format parquet --output-file-format avro".split(" ")

    // -- When
    val bootParams = BootParams.parse(params)

    // -- Then
    bootParams should be(None)
  }

  test("output-compression-format parameter should exist maximum once") {
    // -- Given
    val params = "--input-directory dummyInDir --output-compression-format lzo --output-compression-format gzip".split(" ")

    // -- When
    val bootParams = BootParams.parse(params)

    // -- Then
    bootParams should be(None)
  }

  test("output-file-format parametFr should have only an authorized value") {
    // -- Given
    val params = "--input-directory dummyInDir --output-file-format dummyFF".split(" ")

    // -- When
    val bootParams = BootParams.parse(params)

    // -- Then
    bootParams should be(None)
  }

  test("output-compression-format parameter should have only an authorized value") {
    // -- Given
    val params = "--input-directory dummyInDir --output-compression-format dummyCF".split(" ")

    // -- When
    val bootParams = BootParams.parse(params)

    // -- Then
    bootParams should be(None)
  }

  test("delete-input-files parameter should exist maximum once") {
    // -- Given
    val params = "--input-directory dummyInDir --delete-input-files true --delete-input-files true".split(" ")

    // -- When
    val bootParams = BootParams.parse(params)

    // -- Then
    bootParams should be(None)
  }

}