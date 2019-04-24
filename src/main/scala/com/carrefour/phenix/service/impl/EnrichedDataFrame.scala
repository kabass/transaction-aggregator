package com.carrefour.phenix.service.impl

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions.{col, count, sum, _}

object EnrichedDataFrame {

  implicit class TransactionDataFrame(dataFrame: DataFrame) {

    /**
      * Adding DATE column from DATETIME column
      * @return the dataFrame updated
      */
    def withDateFromDateTime(): DataFrame = {
      dataFrame.withColumn(DATE_FIELD, col(DATETIME_FIELD).substr(0, 8))
    }

    /**
      * Aggregate product transaction at day level
      * @return the dataFrame updated
      */
    def aggregateProductTransaction(): DataFrame = {
      dataFrame
        .groupBy(STORE_FIELD, PRODUCT_FIELD, DATE_FIELD)
        .agg(count(TRANSACTION_FIELD).as(NB_TRANSACTION_FIELD), sum(QUANTITY_FIELD).as(QUANTITY_FIELD))
    }

    /**
      * link price to each transaction from referential
      * @param referentialDataFrame the referential dataFrame
      * @return the dataFrame updated
      */
    def addPrice(referentialDataFrame: DataFrame): DataFrame = {
      dataFrame
        .join(referentialDataFrame, Seq(STORE_FIELD, PRODUCT_FIELD), "inner")
        .withColumn(TOTAL_PRICE_FIELD, col(PRICE_FIELD) * col(QUANTITY_FIELD))
        .drop(PRICE_FIELD)
    }

    /** Windowing dataFrame by given windows key, and keep just first rowNumbers records basing on orderignColumn
      *
      * @param windowKeys the windowKeys collection
      * @param orderignColumn the orderignColumn
      * @param rowNumbers the rowNumber
      * @return the dataFrame updated
      */
    def topColumnWindows(windowKeys: Set[String], orderignColumn: String, rowNumbers: Int): DataFrame = {

      val partitionByKey = Window
        .partitionBy(windowKeys.map(col).toSeq: _*)
        .orderBy(col(orderignColumn).desc)
      val rankByKey = row_number().over(partitionByKey)
      dataFrame.withColumn("rank", rankByKey).filter(s"rank<=$rowNumbers").drop("rank")
    }

    /** Sum operation column group by groupColumns and keep just first rowNumbers records basing on orderignColumn
      *
      * @param groupColumns the group columns collection
      * @param operationColumn the operationColumn
      * @param rowNumbers the rowNumbers
      * @return the dataFrame updated
      */
    def topColumn(groupColumns: Set[String], operationColumn: String, rowNumbers: Int): DataFrame = {
      dataFrame.aggregate(groupColumns, operationColumn).sort(col(operationColumn).desc).limit(rowNumbers)
    }

    /** um operation column group by groupColumns
      *
      * @param groupColumns the group columns collection
      * @param operationColumn operationColumn
      * @return the dataFrame updated
      */
    def aggregate(groupColumns: Set[String], operationColumn: String): DataFrame = {
      dataFrame.groupBy(groupColumns.map(col).toSeq: _*).agg(sum(operationColumn).as(operationColumn))
    }

    /**Check date consistency : if there are many date in transaction file or if date in the file name is different to date in file
      *
      * @param dateInFileName the file Name date
      */
    def checkDateConsistency(dateInFileName: String): Unit = {
      val transactionDateDataFrame = dataFrame.select(DATE_FIELD).distinct()
      val dateInFile               = transactionDateDataFrame.head().getAs[String](DATE_FIELD)
      if (transactionDateDataFrame.count() > 1) {
        throw new RuntimeException("the transaction file contains more than 1 day")
      } else if (!dateInFile.equals(dateInFileName)) {
        throw new RuntimeException(s"the date in the transaction file $dateInFile  is different to date in filename $dateInFileName")
      }
      transactionDateDataFrame.unpersist(true)
    }
  }

}
