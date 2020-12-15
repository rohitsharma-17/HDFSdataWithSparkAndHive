package com.hashedin

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{max, struct}

class SetDataframe {

  def getGrossingTitlesProducedEachYear(moveDetail: DataFrame): DataFrame = {
    moveDetail.where("worlwide_gross_income is NOT NULL")
      .orderBy("year")
      .groupBy("year")
      .agg(max(struct("worlwide_gross_income", "title")).as("all_col"))
      .select("year", "all_col.*")
  }

  def getReviewedTitlesProducedEachYearForAllLanguage(moveDetail: DataFrame): DataFrame = {
    moveDetail.where("language is not null and reviews_from_users is not null")
      .orderBy(sortCol = "year")
      .groupBy("year", "language")
      .agg(max(struct("reviews_from_users", "title", "language")).as(alias = "all_col"))
      .select("year", "all_col.*")
  }

  def getTitlesWithHighestVotesForEachYear(moveDetail: DataFrame): DataFrame = {
    moveDetail.orderBy(sortCol = "year")
      .groupBy("year")
      .agg(max(struct("title", "votes")).as("all_column"))
      .select("year", "all_column.*")
  }
}
