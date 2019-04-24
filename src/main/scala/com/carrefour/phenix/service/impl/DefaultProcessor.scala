package com.carrefour.phenix.service.impl

import java.time.LocalDate

import org.apache.spark.sql.functions.col
import com.carrefour.phenix.model.AggregatedDataFrames
import com.carrefour.phenix.model.AggregationType.{SALE, TURN_OVER}
import com.carrefour.phenix.service.{FileManager, Processor}
import org.slf4j.LoggerFactory
import org.apache.spark.sql.DataFrame
import com.carrefour.phenix.service.impl.EnrichedDataFrame.TransactionDataFrame
class DefaultProcessor(fileManager: FileManager, partitionNumber: Int) extends Processor {

  private lazy val LOGGER = LoggerFactory.getLogger(this.getClass.getName)
  override def buildAndSaveDailyAggregatedTransaction(inputDirectory: String,
                                                      outputDirectory: String,
                                                      referentialFileAbsolutePathByStoreId: Map[String, String],
                                                      transactionDateString: String): DataFrame = {
    val referentialDataFrame =
      fileManager
        .loadCsvReferentialFiles(referentialFileAbsolutePathByStoreId)
        .repartition(partitionNumber, col(STORE_FIELD))
        .checkpoint()
    val transactionDataFrame = fileManager
      .loadCsvFile(
        s"$inputDirectory/transaction/transactions_$transactionDateString*.data",
        header = false,
        Seq(TRANSACTION_FIELD, DATETIME_FIELD, STORE_FIELD, PRODUCT_FIELD, QUANTITY_FIELD)
      )
    if (transactionDataFrame.isEmpty) {
      LOGGER.error(s"no transaction file was found in this path : $inputDirectory/transaction/transactions_$transactionDateString*.data")
      throw new RuntimeException(
        s"no transaction file was found in this path : $inputDirectory/transaction/transactions_$transactionDateString*.data")
    }
    val transactionDataFrameWithDate = transactionDataFrame.get
      .withDateFromDateTime()
    transactionDataFrameWithDate.checkDateConsistency(transactionDateString)
    transactionDataFrameWithDate
      .aggregateProductTransaction()
      .addPrice(referentialDataFrame)
      .repartition(partitionNumber, col(STORE_FIELD))
      .checkpoint()

  }

  override def generateTopDailyAggreated(dailyAggregatedTransaction: DataFrame,
                                         outputDirectory: String,
                                         storeIds: Set[String],
                                         transactionDateString: String,
                                         topLimit: Int): Unit = {
    val dailyTopDataFrames = new AggregatedDataFrames(dailyAggregatedTransaction, topLimit)
    fileManager.saveTopDailyStores(storeIds,
                                   dailyTopDataFrames.topNSalesStoreDataFrame,
                                   outputDirectory,
                                   transactionDateString,
                                   SALE,
                                   topLimit)
    fileManager.saveTopDailyGlobal(dailyTopDataFrames.topNSalesGlobalDataFrame, outputDirectory, transactionDateString, SALE, topLimit)
    fileManager.saveTopDailyStores(
      storeIds,
      dailyTopDataFrames.topNTurnoverStoreDataFrame,
      outputDirectory,
      transactionDateString,
      TURN_OVER,
      topLimit
    )
    fileManager.saveTopDailyGlobal(dailyTopDataFrames.topNTurnoverGlobalDataFrame,
                                   outputDirectory,
                                   transactionDateString,
                                   TURN_OVER,
                                   topLimit)
    dailyTopDataFrames.unpersist()
  }

  override def generateTopPeriodAggreated(outputDirectory: String,
                                          storeIds: Set[String],
                                          transactionDateString: String,
                                          topLimit: Int,
                                          periodNumberOfDays: Int): Unit = {

    val transactionDate = LocalDate.parse(transactionDateString, DATE_FORMATTER)
    val periodTopDataFrames =
      fileManager.loadPeriodDataFrames(outputDirectory, transactionDate, periodNumberOfDays, topLimit)
    fileManager.saveTopPeriodStores(
      storeIds,
      periodTopDataFrames.topNTurnoverStoreDataFrame,
      outputDirectory,
      transactionDateString,
      TURN_OVER,
      topLimit,
      periodNumberOfDays
    )
    fileManager.saveTopPeriodStores(
      storeIds,
      periodTopDataFrames.topNSalesStoreDataFrame,
      outputDirectory,
      transactionDateString,
      SALE,
      topLimit,
      periodNumberOfDays
    )
    fileManager.saveTopPeriodGlobal(periodTopDataFrames.topNTurnoverGlobalDataFrame,
                                    outputDirectory,
                                    transactionDateString,
                                    TURN_OVER,
                                    topLimit,
                                    periodNumberOfDays)
    fileManager.saveTopPeriodGlobal(periodTopDataFrames.topNSalesGlobalDataFrame,
                                    outputDirectory,
                                    transactionDateString,
                                    SALE,
                                    topLimit,
                                    periodNumberOfDays)
    periodTopDataFrames.unpersist()
  }
}
