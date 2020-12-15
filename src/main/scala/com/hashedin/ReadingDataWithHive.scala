package com.hashedin


import org.apache.spark.sql.{SparkSession}


object ReadingDataWithHive {

  def main(args: Array[String]): Unit = {
    val setDataframe = new SetDataframe()
    val spark = SparkSession.builder()
      .master("local")
      .appName("Spark_Hive")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()
    val moveDetails = spark.sql("SELECT * FROM spark_hive.movies_data")

    //Report of top grossing titles produced each year.
    setDataframe.getGrossingTitlesProducedEachYear(moveDetails)
      .write.mode("overwrite").saveAsTable("spark_hive.grossing_titles_produced")

    //Report of top reviewed titles produced each year for all the languages
    setDataframe.getReviewedTitlesProducedEachYearForAllLanguage(moveDetails)
      .write.mode("overwrite").saveAsTable("spark_hive.reviewed_titles_produced")

    //Report award winning titles for each year based on highest votes
    setDataframe.getTitlesWithHighestVotesForEachYear(moveDetails)
      .write.mode("overwrite").saveAsTable("spark_hive.titles_with_highest_votes")
  }
}
