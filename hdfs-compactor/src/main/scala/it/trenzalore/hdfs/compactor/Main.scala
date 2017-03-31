package it.trenzalore.hdfs.compactor

import org.apache.spark.sql.{ SaveMode, SparkSession }
import com.twitter.conversions.storage._
import it.trenzalore.hdfs.compactor.configuration.{ BootParams, CompactionConfiguration }
import it.trenzalore.hdfs.compactor.run.CompactorRunner
import org.apache.hadoop.fs.FileSystem
import org.slf4j.LoggerFactory

object Main {

  lazy val logger = LoggerFactory.getLogger(getClass())

  def main(args: Array[String]) {

    BootParams.parse(args).foreach { bootParams ⇒
      logger.info("HDFS Compactor started with paremeters :\n{}", bootParams)

      implicit val spark = SparkSession
        .builder()
        .appName("hdfs-compactor")
        .master("local[*]")
        .config("spark.ui.enabled", "true")
        .getOrCreate()

      logger.debug("Spark session created")

      implicit val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

      val compactionConfiguration = CompactionConfiguration.compute(bootParams)

      logger.info("Will use compaction configuration : {}", compactionConfiguration)

      CompactorRunner.run(compactionConfiguration)

      logger.info("HDFS Compactor is done !")
    }

  }

}