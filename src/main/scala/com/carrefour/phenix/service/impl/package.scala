package com.carrefour.phenix.service

import java.time.format.DateTimeFormatter

package object impl {
  val PRODUCT_FIELD        = "produit"
  val NB_TRANSACTION_FIELD = "nb_tx"
  val PRICE_FIELD          = "prix"
  val TOTAL_PRICE_FIELD    = "prix_total"
  val STORE_FIELD          = "magasin"
  val TRANSACTION_FIELD    = "txId"
  val DATETIME_FIELD       = "datetime"
  val DATE_FIELD           = "date"
  val QUANTITY_FIELD       = "qte"

  val STORE_SALE_DAILY_DIR       = "/store/top_n_sales/daily/"
  val STORE_TURNOVER_DAILY_DIR   = "/store/top_n_turnover/daily/"
  val GLOBAL_TURNOVER_DAILY_DIR  = "/global/top_n_turnover/daily/"
  val GLOBAL_SALE_DAILY_DIR      = "/global/top_n_sales/daily/"
  val STORE_SALE_PERIOD_DIR      = "/store/top_n_sales/period/"
  val STORE_TURNOVER_PERIOD_DIR  = "/store/top_n_turnover/period/"
  val GLOBAL_TURNOVER_PERIOD_DIR = "/global/top_n_turnover/period/"
  val GLOBAL_SALE_PERIOD_DIR     = "/global/top_n_sales/period/"
  val AGGREGATED_FILE_DIRECTORY  = "/aggregate/transaction/products_by_day/"

  val DATE_FORMATTER: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMdd")
}
