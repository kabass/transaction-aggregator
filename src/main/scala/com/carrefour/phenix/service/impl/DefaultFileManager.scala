package com.carrefour.phenix.service.impl

import java.io.File
import java.time.LocalDate

import com.carrefour.phenix.model.AggregatedDataFrames
import com.carrefour.phenix.service.FileManager
import com.carrefour.phenix.model.AggregationType.{AggregationType, _}
import org.apache.commons.io.FileUtils
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.functions.{col, lit, max}
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}
import org.slf4j.LoggerFactory

class DefaultFileManager(sparkSession: SparkSession, partitionNumber: Int) extends FileManager {
  private lazy val LOGGER = LoggerFactory.getLogger(this.getClass.getName)

  val FILESYSTEM: FileSystem =
    FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
  val DELIMITER = "|"
  val CHARSET   = "UTF-8"

  override def getFileAbsolutePathByStoreId(regex: String, dateString: String): Map[String, String] = {
    try {
      val files = sparkSession.read.textFile(s"$regex/referential/reference_prod*$dateString.data").inputFiles.toSet
      files
        .map(value => (value.replaceFirst(".*reference_prod-", "").replaceFirst("_.*", ""), value))
        .toMap
    } catch {
      case exception: Exception =>
        LOGGER.error(s"no referentail files were found in this path : $regex")
        throw exception

    }
  }

  override def loadCsvReferentialFiles(filePathByStoreId: Map[String, String]): DataFrame = {
    filePathByStoreId
      .map(filePathStore => {
        loadCsvFile(filePathStore._2, header = false, Seq(PRODUCT_FIELD, PRICE_FIELD)).get
          .withColumn(STORE_FIELD, lit(filePathStore._1))
      })
      .reduce(_ union _)
      .groupBy(STORE_FIELD, PRODUCT_FIELD)
      .agg(max(PRICE_FIELD).as(PRICE_FIELD))
  }

  override def saveAggregatedTransaction(dataFrame: DataFrame, outputDirectory: String, date: String): Unit = {
    val filePath = s"$outputDirectory$AGGREGATED_FILE_DIRECTORY/transactions_agg_$date.data"
    writeCoalesce(dataFrame, filePath)
  }

  private def writeCoalesce(dataFrame: DataFrame, filePath: String): Unit = {
    val directory = new Path(filePath).getParent.toString
    val fileName  = new Path(filePath).getName
    val tempPath  = s"${filePath}_temp"
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header", "true")
      .option("delimiter", DELIMITER)
      .option("compression", "none")
      .option("charset", CHARSET)
      .save(tempPath)
    FILESYSTEM.delete(new Path(filePath), true)
    FileUtil.copyMerge(FILESYSTEM,
                       new Path(tempPath),
                       FILESYSTEM,
                       new Path(filePath),
                       false,
                       sparkSession.sparkContext.hadoopConfiguration,
                       null)
    FILESYSTEM.delete(new Path(tempPath), true)
    FILESYSTEM.delete(new Path(s"$directory/.$fileName.crc"), true)
    LOGGER.info(s"$filePath saved")

  }

  override def saveTopDailyStores(storesId: Set[String],
                                  dataFrame: DataFrame,
                                  outputDirectory: String,
                                  date: String,
                                  aggregationType: AggregationType,
                                  topLimitNumber: Int): Unit = {
    val dataFramesByStore = storesId.par
      .map(storeId => {
        (storeId, dataFrame.filter(col(s"$STORE_FIELD").equalTo(storeId)))
      })
      .toMap

    dataFramesByStore.par.foreach(dataFrame => {
      val filePath =
        aggregationType match {
          case SALE      => s"$outputDirectory/$STORE_SALE_DAILY_DIR/top_${topLimitNumber}_ventes_${dataFrame._1}_$date.data"
          case TURN_OVER => s"$outputDirectory/$STORE_TURNOVER_DAILY_DIR/top_${topLimitNumber}_ca_${dataFrame._1}_$date.data"
        }
      writeCoalesce(dataFrame._2, filePath)
    })

  }

  override def saveTopDailyGlobal(dataFrame: DataFrame,
                                  outputDirectory: String,
                                  date: String,
                                  aggregationType: AggregationType,
                                  topLimitNumber: Int): Unit = {

    val filePath =
      aggregationType match {
        case SALE      => s"$outputDirectory/$GLOBAL_SALE_DAILY_DIR/top_${topLimitNumber}_ventes_GLOBAL_$date.data"
        case TURN_OVER => s"$outputDirectory/$GLOBAL_TURNOVER_DAILY_DIR/top_${topLimitNumber}_ca_GLOBAL_$date.data"
      }
    writeCoalesce(dataFrame, filePath)

  }

  override def saveTopPeriodStores(storesId: Set[String],
                                   dataFrame: DataFrame,
                                   outputDirectory: String,
                                   endDate: String,
                                   aggregationType: AggregationType,
                                   topLimitNumber: Int,
                                   numberOdDays: Int): Unit = {
    storesId.par.foreach(storeId => {
      val filePath =
        aggregationType match {
          case SALE      => s"$outputDirectory/$STORE_SALE_PERIOD_DIR/top_${topLimitNumber}_ventes_${storeId}_$endDate-J$numberOdDays.data"
          case TURN_OVER => s"$outputDirectory/$STORE_TURNOVER_PERIOD_DIR/top_${topLimitNumber}_ca_${storeId}_$endDate-J$numberOdDays.data"
        }
      writeCoalesce(dataFrame.filter(col(s"$STORE_FIELD").equalTo(storeId)), filePath)
    })

  }

  override def saveTopPeriodGlobal(dataFrame: DataFrame,
                                   outputDirectory: String,
                                   endDate: String,
                                   aggregationType: AggregationType,
                                   topLimitNumber: Int,
                                   numberOdDays: Int): Unit = {

    val filePath =
      aggregationType match {
        case SALE      => s"$outputDirectory/$GLOBAL_SALE_PERIOD_DIR/top_${topLimitNumber}_ventes_GLOBAL_$endDate-J$numberOdDays.data"
        case TURN_OVER => s"$outputDirectory/$GLOBAL_TURNOVER_PERIOD_DIR/top_${topLimitNumber}_ca_GLOBAL_$endDate-J$numberOdDays.data"
      }
    writeCoalesce(dataFrame, filePath)

  }

  override def loadPeriodDataFrames(outputPath: String, date: LocalDate, numberOfDays: Int, topLimitNumber: Int): AggregatedDataFrames = {

    val periodTopNSalesStoreFiles = (0 until numberOfDays)
      .map(value =>
        s"$outputPath/$STORE_SALE_DAILY_DIR/top_${topLimitNumber}_ventes_*_${date.minusDays(value).format(DATE_FORMATTER)}.data")
      .toSet
    val periodTopNTurnoverStoreFiles = (0 until numberOfDays)
      .map(value =>
        s"$outputPath/$STORE_TURNOVER_DAILY_DIR/top_${topLimitNumber}_ca_*_${date.minusDays(value).format(DATE_FORMATTER)}.data")
      .toSet
    val periodTopNSalesGlobalFiles = (0 until numberOfDays)
      .map(value =>
        s"$outputPath/$GLOBAL_SALE_DAILY_DIR/top_${topLimitNumber}_ventes_GLOBAL_${date.minusDays(value).format(DATE_FORMATTER)}.data")
      .toSet
    val periodTopNTurnoverGlobalFiles = (0 until numberOfDays)
      .map(value =>
        s"$outputPath/$GLOBAL_TURNOVER_DAILY_DIR/top_${topLimitNumber}_ca_GLOBAL_${date.minusDays(value).format(DATE_FORMATTER)}.data")
      .toSet
    val periodTopNSalesStoreDataFrame: DataFrame =
      periodTopNSalesStoreFiles
        .map(loadCsvFile(_))
        .filter(_.isDefined)
        .map(_.get)
        .reduce(_ union _)
        .repartition(partitionNumber, col(STORE_FIELD))
        .checkpoint()

    val periodTopNTurnoverStoreDataFrame: DataFrame =
      periodTopNTurnoverStoreFiles
        .map(loadCsvFile(_))
        .filter(_.isDefined)
        .map(_.get)
        .reduce(_ union _)
        .repartition(partitionNumber, col(STORE_FIELD))
        .checkpoint()
    val periodTopNSalesGlobalDataFrame: DataFrame =
      periodTopNSalesGlobalFiles
        .map(loadCsvFile(_))
        .filter(_.isDefined)
        .map(_.get)
        .reduce(_ union _)
        .repartition(partitionNumber)
        .checkpoint()
    val periodTopNTurnoverGlobalDataFrame: DataFrame =
      periodTopNTurnoverGlobalFiles
        .map(loadCsvFile(_))
        .filter(_.isDefined)
        .map(_.get)
        .reduce(_ union _)
        .repartition(partitionNumber)
        .checkpoint()

    new AggregatedDataFrames(
      periodTopNSalesStoreDataFrame,
      periodTopNTurnoverStoreDataFrame,
      periodTopNSalesGlobalDataFrame,
      periodTopNTurnoverGlobalDataFrame,
      topLimitNumber
    )
  }

  override def loadCsvFile(filePath: String, header: Boolean = true, columns: Seq[String] = Seq()): Option[DataFrame] = {
    try {
      val inferSchema = true
      var dataFrame = sparkSession.read
        .format("csv")
        .option("delimiter", DELIMITER)
        .option("charset", CHARSET)
        .option("inferSchema", inferSchema)
        .option("header", header)
        .load(s"$filePath")
      if (!header) {
        dataFrame = dataFrame.toDF(columns: _*)
      }
      Option.apply(dataFrame)
    } catch {
      case _: AnalysisException => Option.empty
      case exception: Exception => throw exception
    }

  }

  /** To build and save dataFrame with transaction aggregated by product , store and date
    *
    * @param inputDirectory        : the input directory path,
    * @param archiveDirectory       : the output directory path
    * @param transactionDateString : the transaction date
    * @param referential_files : the referential files
    * @return the dataFrame
    */
  override def archiveInput(inputDirectory: String,
                            archiveDirectory: String,
                            transactionDateString: String,
                            referential_files: Set[String]): Unit = {
    FileUtils.forceMkdir(new File(s"$archiveDirectory/referential"))
    FileUtils.forceMkdir(new File(s"$archiveDirectory/transaction"))

    FileUtil.copy(
      FILESYSTEM,
      Array(new Path(s"$inputDirectory/transaction/transactions_$transactionDateString.data")),
      FILESYSTEM,
      new Path(s"$archiveDirectory/transaction"),
      true,
      true,
      sparkSession.sparkContext.hadoopConfiguration
    )

    FileUtil.copy(
      FILESYSTEM,
      referential_files.map(new Path(_)).toArray,
      FILESYSTEM,
      new Path(s"$archiveDirectory/referential"),
      true,
      true,
      sparkSession.sparkContext.hadoopConfiguration
    )

    FILESYSTEM.delete(new Path(s"$archiveDirectory/transaction/.transactions_$transactionDateString.data.crc"), true)
    referential_files.foreach(file => {
      val fileName = s".${new File(file).getName}.crc"
      FILESYSTEM.delete(new Path(s"$archiveDirectory/referential/$fileName"), true)
    })
  }
}
