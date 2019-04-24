package com.carrefour.phenix.model

import com.carrefour.phenix.service.impl.EnrichedDataFrame.TransactionDataFrame
import com.carrefour.phenix.service.impl._
import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel

case class AggregatedDataFrames() {
  var topNSalesStoreDataFrame: DataFrame     = _
  var topNTurnoverStoreDataFrame: DataFrame  = _
  var topNSalesGlobalDataFrame: DataFrame    = _
  var topNTurnoverGlobalDataFrame: DataFrame = _

  def this(previousTopNSalesStoreDataFrame: DataFrame,
           previousTopNTurnoverStoreDataFrame: DataFrame,
           previousTopNSalesGlobalDataFrame: DataFrame,
           previousTopNTurnoverGlobalDataFrame: DataFrame,
           topLimitNumber: Int) = {
    this()
    this.topNSalesStoreDataFrame = previousTopNSalesStoreDataFrame
      .aggregate(Set(STORE_FIELD, PRODUCT_FIELD), QUANTITY_FIELD)
      .topColumnWindows(Set(STORE_FIELD), QUANTITY_FIELD, topLimitNumber)
      .cache()

    this.topNTurnoverStoreDataFrame = previousTopNTurnoverStoreDataFrame
      .aggregate(Set(STORE_FIELD, PRODUCT_FIELD), TOTAL_PRICE_FIELD)
      .topColumnWindows(Set(STORE_FIELD), TOTAL_PRICE_FIELD, topLimitNumber)
      .cache()

    this.topNSalesGlobalDataFrame = previousTopNSalesGlobalDataFrame
      .topColumn(Set(PRODUCT_FIELD), QUANTITY_FIELD, topLimitNumber)
      .cache()
    this.topNTurnoverGlobalDataFrame = previousTopNTurnoverGlobalDataFrame
      .topColumn(Set(PRODUCT_FIELD), TOTAL_PRICE_FIELD, topLimitNumber)
      .cache()
  }

  def this(dailyTransactionAggregated: DataFrame, topLimitNumber: Int) = {
    this()
    this.topNSalesStoreDataFrame = dailyTransactionAggregated
      .topColumnWindows(Set(STORE_FIELD, DATE_FIELD), QUANTITY_FIELD, topLimitNumber)
      .checkpoint()
    this.topNSalesGlobalDataFrame = topNSalesStoreDataFrame
      .topColumn(Set(PRODUCT_FIELD, DATE_FIELD), QUANTITY_FIELD, topLimitNumber)
      .checkpoint()
    this.topNTurnoverStoreDataFrame = dailyTransactionAggregated
      .topColumnWindows(Set(STORE_FIELD, DATE_FIELD), TOTAL_PRICE_FIELD, topLimitNumber)
      .checkpoint()
    this.topNTurnoverGlobalDataFrame = topNTurnoverStoreDataFrame
      .topColumn(Set(PRODUCT_FIELD, DATE_FIELD), TOTAL_PRICE_FIELD, topLimitNumber)
      .checkpoint()
  }

  def unpersist(): Unit = {
    this.topNSalesStoreDataFrame.unpersist()
    this.topNSalesGlobalDataFrame.unpersist()
    this.topNTurnoverStoreDataFrame.unpersist()
    this.topNTurnoverGlobalDataFrame.unpersist()
  }
}
