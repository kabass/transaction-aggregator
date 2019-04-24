package com.carrefour.phenix.service

import java.time.LocalDate

import com.carrefour.phenix.model.AggregatedDataFrames
import com.carrefour.phenix.model.AggregationType.AggregationType
import org.apache.spark.sql.DataFrame

trait FileManager {

  /** Get all filePaths regarding the file path regex
    *
    * @param regex The filepath regex
    * @param dateString the transaction date to process
    * @return the file absolute paths by store ID
    */
  def getFileAbsolutePathByStoreId(regex: String, dateString: String): Map[String, String]

  /** load referential files, add store id column for each referential data d union all referential data
    *
    * @param filePathByStoreId tthe file path by store ID
    * @return the dataframe containing the union result of all referential
    */
  def loadCsvReferentialFiles(filePathByStoreId: Map[String, String]): DataFrame

  /** load csv File
    *
    * @param filePath tthe file path
    * @param header   the header presence
    * @param columns  the sequence of column in order
    * @return the dataframe
    */
  def loadCsvFile(filePath: String, header: Boolean = true, columns: Seq[String] = Seq()): Option[DataFrame]

  /** To load the number of days previous datagframe
    *
    * @param outputPath     the ouput path where the previous data were saved
    * @param date           the date
    * @param numberOfDays   the number of previous days
    * @param topLimitNumber the number of top record to keep
    * @return the dataframes
    */
  def loadPeriodDataFrames(outputPath: String, date: LocalDate, numberOfDays: Int, topLimitNumber: Int): AggregatedDataFrames

  /** To save the aggregated by products by day
    *
    * @param dataFrame       the aggregated transaction dataframe
    * @param outputDirectory the output directory
    * @param date            the date
    */
  def saveAggregatedTransaction(dataFrame: DataFrame, outputDirectory: String, date: String): Unit

  /** To save daily top 100  for each stores
    *
    * @param storesId        the store ids
    * @param dataFrame       the aggregated transaction dataframe
    * @param outputDirectory the output directory
    * @param date            the date
    * @param aggregationType the aggregation type
    * @param topLimitNumber  the number of top record to keep
    *
    */
  def saveTopDailyStores(storesId: Set[String],
                         dataFrame: DataFrame,
                         outputDirectory: String,
                         date: String,
                         aggregationType: AggregationType,
                         topLimitNumber: Int): Unit

  /** To save period top N  for all stores
    *
    * @param dataFrame       the aggregated transaction dataframe
    * @param outputDirectory the output directory
    * @param date            the date
    * @param aggregationType the aggregation type
    * @param topLimitNumber  the number of top record to keep
    *
    */
  def saveTopDailyGlobal(dataFrame: DataFrame,
                         outputDirectory: String,
                         date: String,
                         aggregationType: AggregationType,
                         topLimitNumber: Int): Unit

  /** To save period top N  for each stores
    *
    * @param dataFrame       the aggregated transaction dataframe
    * @param outputDirectory the output directory
    * @param endDate         the period end date
    * @param aggregationType the aggregation type
    * @param topLimitNumber  the number of top record to keep
    * @param numberOdDays    the number of days of period
    *
    */
  def saveTopPeriodStores(storesId: Set[String],
                          dataFrame: DataFrame,
                          endDate: String,
                          outputDirectory: String,
                          aggregationType: AggregationType,
                          topLimitNumber: Int,
                          numberOdDays: Int): Unit

  /** To save period top N  for all stores
    *
    * @param dataFrame       the aggregated transaction dataframe
    * @param outputDirectory the output directory
    * @param endDate         the period end date
    * @param aggregationType the aggregation type
    * @param topLimitNumber  the number of top record to keep
    * @param numberOdDays    the number of days of period
    *
    */
  def saveTopPeriodGlobal(dataFrame: DataFrame,
                          outputDirectory: String,
                          endDate: String,
                          aggregationType: AggregationType,
                          topLimitNumber: Int,
                          numberOdDays: Int): Unit

  /** To build and save dataFrame with transaction aggregated by product , store and date
    *
    * @param inputDirectory        : the input directory path,
    * @param archiveDirectory       : the output directory path
    * @param transactionDateString : the transaction date
    * @param referential_files : the referential files
    * @return the dataFrame
    */
  def archiveInput(inputDirectory: String, archiveDirectory: String, transactionDateString: String, referential_files: Set[String]): Unit

}
