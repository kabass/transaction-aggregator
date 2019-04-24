package com.carrefour.phenix

import java.time.LocalDate

import com.beust.jcommander.JCommander
import com.carrefour.phenix.service.impl.{DefaultFileManager, _}
import com.carrefour.phenix.util.CommandLineArgs
import org.apache.spark.sql.SparkSession
import org.slf4j.LoggerFactory

object Application extends App {

  private lazy val LOGGER = LoggerFactory.getLogger(this.getClass.getName)

  setConfig(args)
  val sparkSession = SparkSession
    .builder()
    .appName("transaction aggregator")
    //.master("local[2]")
    //.config("spark.executor.memory", "512m")
    //.config("spark.driver.memory", "512m")
    //.config("spark.executor.cores", "2")
    //.config("spark.eventLog.enabled", true)
    //.config("spark.eventLog.dir", "/home/bka/logiciel/hadoop-spark/log")
    .getOrCreate()
  sparkSession.sparkContext.setCheckpointDir("/tmp/checkpoint")

  val inputDirectory        = CommandLineArgs.inputDirectory
  val outputDirectory       = CommandLineArgs.outputDirectory
  val archiveDirectory      = if (CommandLineArgs.archiveDirectory == null) Option.empty else Option.apply(CommandLineArgs.archiveDirectory)
  val transactionDateString = CommandLineArgs.transactionDateString
  val topLimit              = CommandLineArgs.topLimit
  val periodLength          = CommandLineArgs.periodLength
  val partitionNumber       = CommandLineArgs.periodLength

  val fileManager = new DefaultFileManager(sparkSession, partitionNumber)
  val processor   = new DefaultProcessor(fileManager, partitionNumber)

  val fileAbsolutePathByStoreId = fileManager.getFileAbsolutePathByStoreId(CommandLineArgs.inputDirectory, transactionDateString)

  val dailyAggregatedTransaction =
    processor.buildAndSaveDailyAggregatedTransaction(inputDirectory, outputDirectory, fileAbsolutePathByStoreId, transactionDateString)
  processor.generateTopDailyAggreated(dailyAggregatedTransaction,
                                      outputDirectory,
                                      fileAbsolutePathByStoreId.keySet,
                                      transactionDateString,
                                      topLimit)
  processor.generateTopPeriodAggreated(outputDirectory, fileAbsolutePathByStoreId.keySet, transactionDateString, topLimit, periodLength)

  if (archiveDirectory.isDefined) {
    fileManager.archiveInput(inputDirectory, archiveDirectory.get, transactionDateString, fileAbsolutePathByStoreId.values.toSet)
  }

  def setConfig(args: Array[String]): Unit = {

    val jCommander = new JCommander(CommandLineArgs, null, args.toArray: _*)
    if (CommandLineArgs.help) {
      jCommander.usage()
      System.exit(0)
    }
    LOGGER.info(s"config loaded : $CommandLineArgs")

  }
}
