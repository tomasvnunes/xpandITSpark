package utils

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileUtils {

  val logger = Logger.getLogger(this.getClass.getName)
  
  def loadCsv(sparkSession: SparkSession, path: String): DataFrame = {
    logger.info("Loading data from " + path)
    val dataFrame = sparkSession.read.option("header", true).csv(path)
    logger.info("Data loaded")
    dataFrame;
  }

  def saveToCsv(sparkSession: SparkSession, df: DataFrame, folder: String, filename: String): Unit = {
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", true)
      .option("delimiter", "§")
      .csv(folder)
    deleteUnrelatedFiles(sparkSession, folder, filename)
    logger.info("Dataframe saved to " + folder + "/" + filename)
  }

  def saveToParquet(sparkSession: SparkSession, df: DataFrame, folder: String, filename: String): Unit = {
    df.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", true)
      .option("compression", "gzip")
      .parquet(folder)
    deleteUnrelatedFiles(sparkSession, folder, filename)
    logger.info("Dataframe saved to " + folder + "/" + filename)
  }

  private def deleteUnrelatedFiles(sparkSession: SparkSession, folder: String, filename: String): Unit = {
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    val files = fs.listFiles(new Path(folder), false)
    while (files.hasNext) {
      val curFile = files.next()
      if (curFile.getPath.getName.contains("part-0000")) {
        fs.rename(new Path(folder + "/" + curFile.getPath.getName), new Path(folder + "/" + filename))
      } else {
        fs.delete(curFile.getPath, true)
      }
    }
  }
}
