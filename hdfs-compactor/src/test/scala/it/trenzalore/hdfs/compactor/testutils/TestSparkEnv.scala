package it.trenzalore.hdfs.compactor.testutils

import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs.FileSystem

object TestSparkEnv {

  lazy val spark = SparkSession
    .builder()
    .appName("hdfs-compactor-test")
    .master("local[*]")
    .getOrCreate()

  lazy val hadoopConfiguration = spark.sparkContext.hadoopConfiguration

  lazy val fs = FileSystem.get(hadoopConfiguration)

}