package com.hashedin

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object ReadingDataWithSpark {

  def readerForHDFS(hdfsFileLocation: String): DataFrame = {
    val conf = new SparkConf().setAppName("IDMB movie csv reader").setMaster("local[*]")
    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    sparkSession.read.format("csv")
      .option("delimiter", "\t")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(hdfsFileLocation)
  }

  def main(args: Array[String]) {

    val setDataframe = new SetDataframe()
    val hdfsPath = "hdfs://localhost:9000/user/hasher/"
    val hdfsOutPath = "hdfs://localhost:9000/user/hasher/output/"
    val hdfsFile = "imdb_movies.csv"
    val moveDetails = readerForHDFS(hdfsPath + hdfsFile)

    //Report of top reviewed titles produced each year for all the languages
    setDataframe.getReviewedTitlesProducedEachYearForAllLanguage(moveDetails)
      .coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(hdfsOutPath+"reviewed_titles_produced_all_language.csv")

    //Report of top grossing titles produced each year.
    setDataframe.getGrossingTitlesProducedEachYear(moveDetails)
      .coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(hdfsOutPath+"grossing_titles_produced.csv")

    //Report award winning titles for each year based on highest votes
    setDataframe.getTitlesWithHighestVotesForEachYear(moveDetails)
      .coalesce(1).write.mode("overwrite")
      .option("header", "true").csv(hdfsOutPath+"titles_with_highest_votes.csv")

  }
}
