import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import processor.DataProcessor
import utils.Constants.DefaultMasterOption
import utils.FileUtils.loadCsv

object Main {
  val logger = Logger.getLogger(this.getClass.getName)

  def main(args: Array[String]): Unit = {
    val usageMessage = "Arguments order: <users_reviews_csv_path> <playstore_apps_csv_path> <optional:master_option>"
    logger.info("Starting Xpand IT Spark & Scala Challenge")
    var master = DefaultMasterOption
    if (args.length < 2) {
      logger.error("Insufficient args passed. Please provide the file paths you want to load. Optionally, master settings can also be passed in the last parameter\n" + usageMessage)
      return;
    }
    if (args.length == 2) {
      logger.warn("No master option passed. Defaulting to: " + master)
    }
    else {
      master = args(2)
    }
    val spark = SparkSession.builder()
      .master(master)
      .appName("Spark & Scala demo")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val usersReviewsPath = getClass.getResource(args(0)).getPath
    val userReviews = loadCsv(spark, usersReviewsPath)
    val playstoreAppsPath = getClass.getResource(args(1)).getPath;
    val apps = loadCsv(spark, playstoreAppsPath)
    val dataProcessor = new DataProcessor(spark, userReviews, apps)
    dataProcessor.process()

  }

}
