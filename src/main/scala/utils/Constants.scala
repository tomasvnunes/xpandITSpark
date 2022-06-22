package utils

object Constants {

  val DefaultMasterOption = "local[*]"

  val MB = "M"
  val KB = "k"
  val VaryingMessage = "Varies"

  object OriginalColumns {
    val App = "App"
    val SentimentPolarity = "Sentiment_Polarity"
    val Rating = "Rating"
    val Reviews = "Reviews"
    val Category = "Category"
    val Categories = "Categories"
    val Size = "Size"
    val Installs = "Installs"
    val Type = "Type"
    val Price = "Price"
    val ContentRating = "Content Rating"
    val Genres = "Genres"
    val LastUpdated = "Last Updated"
    val CurrentVersion = "Current Ver"
    val AndroidVersion = "Android Ver"

  }

  object CustomColumns {
    val AverageSentimentPolarity = "Average_Sentiment_Polarity"
    val ContentRating = "Content_Rating"
    val LastUpdated = "Last_Updated"
    val CurrentVersion = "Current_Version"
    val MinAndroidVersion = "Minimum_Android_Version"
    val Genre = "Genre"
    val Count = "Count"
    val AverageRating = "Average_Rating"
  }

  object Files {
    private val CommonOutputFolder = "output/"

    val BestAppsFile = "best_apps.csv"
    val BestAppsFolder: String = CommonOutputFolder+"best"
    val CleanedAppsFile = "googleplaystore_cleaned"
    val CleanedAppsFolder: String = CommonOutputFolder+"cleaned"
  }


}
