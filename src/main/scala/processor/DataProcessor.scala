package processor

import org.apache.log4j.Logger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, LongType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import utils.Constants.Files.{CleanedAppsFile, CleanedAppsFolder}
import utils.Constants._
import utils.FileUtils.{saveToCsv, saveToParquet}

class DataProcessor(var spark: SparkSession, var userReviews: DataFrame, var apps: DataFrame) {

  val logger = Logger.getLogger(this.getClass.getName)

  def process(): Unit = {
    logger.debug("Amount of apps: " + apps.count())
    // PART 1
    logger.info("Challenge part 1:")
    userReviews.select(col(OriginalColumns.App),
      coalesce(col(OriginalColumns.SentimentPolarity).cast(DoubleType), lit(0))
        .name(OriginalColumns.SentimentPolarity))
      .na.fill(0)
    val appsReviews = userReviews
      .groupBy("App").agg(avg(OriginalColumns.SentimentPolarity).as(CustomColumns.AverageSentimentPolarity))
    peekDataframe(appsReviews)

    // PART 2
    logger.info("Challenge part 2:")
    var best_apps = apps.withColumn(OriginalColumns.Rating,
      coalesce(col(OriginalColumns.Rating).cast(DoubleType), lit(0)).name(OriginalColumns.Rating))
      .na.fill(0) //there is a Rating of 19 on the original data
    best_apps = apps.filter(col(OriginalColumns.Rating) >= 4).orderBy(desc(OriginalColumns.Rating))
    peekDataframe(best_apps)
    saveToCsv(spark, best_apps, Files.BestAppsFolder, Files.BestAppsFile)

    // PART 3
    logger.info("Challenge part 3:")
    val grouped_by_app = apps.orderBy(desc(OriginalColumns.Reviews)) //this order by allows that for all columns except for category, the first value is the one from the entry with most reviews
      .groupBy(OriginalColumns.App)
      .agg(collect_set(OriginalColumns.Category).as(OriginalColumns.Categories),
        first(OriginalColumns.Rating).cast(LongType).as(OriginalColumns.Rating),
        first(OriginalColumns.Reviews).cast(DoubleType).as(OriginalColumns.Reviews),
        first(OriginalColumns.Size).as(OriginalColumns.Size), //convert from string to double
        first(OriginalColumns.Installs).as(OriginalColumns.Installs),
        first(OriginalColumns.Type).as(OriginalColumns.Type),
        first(OriginalColumns.Price).cast(DoubleType).*(0.9).as(OriginalColumns.Price),
        first(OriginalColumns.ContentRating).as(CustomColumns.ContentRating),
        split(first(OriginalColumns.Genres), ";").as(OriginalColumns.Genres),
        first(OriginalColumns.LastUpdated).as(CustomColumns.LastUpdated),
        first(OriginalColumns.CurrentVersion).as(CustomColumns.CurrentVersion),
        first(OriginalColumns.AndroidVersion).as(CustomColumns.MinAndroidVersion))
      .na.fill(0, Seq(OriginalColumns.Reviews))
      .withColumn(CustomColumns.LastUpdated, to_date(col(CustomColumns.LastUpdated), "MMM dd, yyyy"))
      .withColumn(OriginalColumns.Size,
        when(col(OriginalColumns.Size).endsWith(MB), split(col(OriginalColumns.Size), "M").getItem(0))
          .when(col(OriginalColumns.Size).endsWith(KB), split(col(OriginalColumns.Size), "k").getItem(0)./(scala.math.pow(10, 3))) //kb to mb
          .when(col(OriginalColumns.Size).contains(VaryingMessage), null) //remove this?
          .otherwise(col(OriginalColumns.Size)).cast(DoubleType))
    peekDataframe(grouped_by_app)

    logger.info("Challenge part 4:")
    val joined_apps = grouped_by_app.join(appsReviews, OriginalColumns.App)
    peekDataframe(joined_apps)
    saveToParquet(spark, joined_apps, CleanedAppsFolder, CleanedAppsFile)


    logger.info("Challenge part 5:")
    val exploded = grouped_by_app.join(userReviews, OriginalColumns.App).select(explode(col(OriginalColumns.Genres)).as(CustomColumns.Genre), col(OriginalColumns.Rating), col(OriginalColumns.SentimentPolarity))
      .groupBy(CustomColumns.Genre)
      .agg(count(CustomColumns.Genre).as(CustomColumns.Count), avg(OriginalColumns.Rating).as(CustomColumns.AverageRating), avg(OriginalColumns.SentimentPolarity).as(CustomColumns.AverageSentimentPolarity))
      .orderBy(desc(CustomColumns.Count))
    peekDataframe(exploded)

  }

  private def peekDataframe(df: DataFrame): Unit = {
    df.show(10)
    df.printSchema()
  }
}
