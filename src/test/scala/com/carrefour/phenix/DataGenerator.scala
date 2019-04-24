package com.carrefour.phenix

import java.io.{BufferedWriter, FileWriter}
import java.time.LocalDate
import java.util.UUID.randomUUID

import au.com.bytecode.opencsv.CSVWriter
import com.carrefour.phenix.service.impl.DATE_FORMATTER

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Random

object DataGenerator extends App {
  val stores = (1 to 1200)
    .map((_, randomUUID().toString))

  val all_product       = (1 to 3000000).map((_, Math.random() * 100)).toList
  var listOfTransaction = new ListBuffer[Array[String]]()
  val random            = new scala.util.Random
  stores.par
    .foreach(value => {

      val outputFile  = new BufferedWriter(new FileWriter(s"${args(0)}/input/referential/reference_prod-${value._2}_20170514.data"))
      val csvWriter   = new CSVWriter(outputFile, '|', 0)
      val referential = Random.shuffle(all_product).take(50000)
      val list        = referential.map(record => Array(record._1.toString, record._2.toString)).toList
      csvWriter.writeAll(list)
      outputFile.close()
      listOfTransaction ++= (1 to 10000).map(transact => {
        val index = random.nextInt(49999) + 1
        Array(transact.toString,
              "20170514T223544+0100",
              value._2.toString,
              referential(index)._1.toString,
              (random.nextInt(5) + 1).toString)
      })

    })
  val transactionFile = new BufferedWriter(new FileWriter(s"${args(0)}/input/transaction/transactions_20170514.data"))
  val csvWriter       = new CSVWriter(transactionFile, '|', 0)
  println(listOfTransaction.length)
  csvWriter.writeAll(listOfTransaction)
  transactionFile.close()

  val all_top_sale_product     = (1 to 5000).map((_, (Math.random() * 5000).toInt)).toList
  val all_top_turnover_product = (1 to 5000).map((_, Math.random() * 50000)).toList
  val dateInit                 = LocalDate.of(2017, 5, 13)
  (0 until 7).foreach(index => {
    val date = dateInit.minusDays(index).format(DATE_FORMATTER)
    stores.par
      .foreach(value => {
        val outputFileTurnover =
          new BufferedWriter(new FileWriter(s"${args(0)}/output/store/top_n_turnover/daily/top_100_ca_${value._2}_$date.data"))
        val outputFileSale =
          new BufferedWriter(new FileWriter(s"${args(0)}/output/store/top_n_sales/daily/top_100_ventes_${value._2}_$date.data"))
        val csvWriterSale     = new CSVWriter(outputFileSale, '|', 0)
        val csvWriterTurnover = new CSVWriter(outputFileTurnover, '|', 0)
        val topSales          = Random.shuffle(all_top_sale_product).take(100)
        val topTurnover       = Random.shuffle(all_top_sale_product).take(100)
        val listSales         = topSales.map(record => Array(value._2.toString, record._1.toString, date, "4", record._2.toString, "1"))
        csvWriterSale.writeNext("magasin", "produit", "date", "nb_tx", "qte", "prix_total")
        csvWriterSale.writeAll(listSales)
        outputFileSale.close()
        val listTurnover = topTurnover.map(record => Array(value._2.toString, record._1.toString, date, "4", "1", record._2.toString))
        csvWriterTurnover.writeNext("magasin", "produit", "date", "nb_tx", "qte", "prix_total")
        csvWriterTurnover.writeAll(listTurnover)
        outputFileTurnover.close()

      })
    val outputFileTurnover =
      new BufferedWriter(new FileWriter(s"${args(0)}/output/global/top_n_turnover/daily/top_100_ca_GLOBAL_$date.data"))
    val outputFileSale =
      new BufferedWriter(new FileWriter(s"${args(0)}/output/global/top_n_sales/daily/top_100_ventes_GLOBAL_$date.data"))
    val csvWriterSale     = new CSVWriter(outputFileSale, '|', 0)
    val csvWriterTurnover = new CSVWriter(outputFileTurnover, '|', 0)
    val topSales          = Random.shuffle(all_top_sale_product).take(100)
    val topTurnover       = Random.shuffle(all_top_sale_product).take(100)
    val listSales         = topSales.map(record => Array(record._1.toString, date, record._2.toString))
    csvWriterSale.writeNext("produit", "date", "qte")
    csvWriterSale.writeAll(listSales)
    outputFileSale.close()
    val listTurnover = topTurnover.map(record => Array(record._1.toString, date, record._2.toString))
    csvWriterTurnover.writeNext("produit", "date", "prix_total")
    csvWriterTurnover.writeAll(listTurnover)
    outputFileTurnover.close()
  })
}
