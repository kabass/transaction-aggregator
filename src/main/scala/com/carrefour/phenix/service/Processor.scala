package com.carrefour.phenix.service

import org.apache.spark.sql.DataFrame

trait Processor {

  /** To build and save dataFrame with transaction aggregated by product , store and date
    *
    * @param inputDirectory : the input directory path,
    * @param outputDirectory : the output directory path,
    * @param referentialFileAbsolutePathByStoreId the referential file by store id
    * @param transactionDateString the date to find transaction file
    * @return the dataFrame
    */
  def buildAndSaveDailyAggregatedTransaction(inputDirectory: String,
                                             outputDirectory: String,
                                             referentialFileAbsolutePathByStoreId: Map[String, String],
                                             transactionDateString: String): DataFrame

  /** Build and save top daily data
    *
    * @param dailyAggregatedTransaction the dailyAggregatedTransaction
    * @param outputDirectory : the output directory path,
    * @param storeIds the store ids
    * @param transactionDateString the transactionDateString
    * @param topLimit the topLimit
    */
  def generateTopDailyAggreated(dailyAggregatedTransaction: DataFrame,
                                outputDirectory: String,
                                storeIds: Set[String],
                                transactionDateString: String,
                                topLimit: Int): Unit

  /** Build and save top daily data
    *
    * @param outputDirectory : the output directory path,
    * @param storeIds the store ids
    * @param transactionDateString the transactionDateString
    * @param topLimit the topLimit
    * @param periodNumberOfDays the periodnumberOfDays
    */
  def generateTopPeriodAggreated(outputDirectory: String,
                                 storeIds: Set[String],
                                 transactionDateString: String,
                                 topLimit: Int,
                                 periodNumberOfDays: Int): Unit

}
