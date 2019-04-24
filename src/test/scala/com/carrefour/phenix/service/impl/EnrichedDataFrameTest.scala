package com.carrefour.phenix.service.impl

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest._
import com.carrefour.phenix.service.impl.EnrichedDataFrame._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, count, row_number, sum}

class EnrichedDataFrameTest extends FeatureSpec with BeforeAndAfterAll with Matchers {
  val sparkSession = SparkSession
    .builder()
    .master("local[2]")
    .getOrCreate()
  import sparkSession.implicits._
  feature("dataFrameWithDateTime function test ") {
    scenario("ALL_OK") {
      val dataFrameWithDateTime = Seq(("n1", "20170514T223544+0100"), ("n2", "20170515T223544+0100"))
        .toDF("name", DATETIME_FIELD)
      val dataFrame = dataFrameWithDateTime.withDateFromDateTime()
      dataFrame.where("name ='n1'").head.getAs[String](DATE_FIELD) should equal("20170514")
      dataFrame.count should equal(2)
    }
  }

  feature("aggregateProductTransaction function test ") {
    scenario("ALL_OK") {
      val transaction =
        Seq(("s1", "p1", "20170515", "t1", 5), ("s1", "p1", "20170515", "t2", 6), ("s1", "p2", "20170515", "t2", 1))
          .toDF(STORE_FIELD, PRODUCT_FIELD, DATE_FIELD, TRANSACTION_FIELD, QUANTITY_FIELD)
      val dataFrame = transaction.aggregateProductTransaction()
      dataFrame.where(s"$STORE_FIELD ='s1' AND $PRODUCT_FIELD='p1'").head.getAs[Int](NB_TRANSACTION_FIELD) should equal(2)
      dataFrame.where(s"$STORE_FIELD ='s1' AND $PRODUCT_FIELD='p1'").head.getAs[Int](QUANTITY_FIELD) should equal(11)
      dataFrame.where(s"$STORE_FIELD ='s1' AND $PRODUCT_FIELD='p2'").head.getAs[Int](QUANTITY_FIELD) should equal(1)
      dataFrame.count should equal(2)
    }
  }

  feature("addPrice function test ") {
    scenario("ALL_OK") {
      val transaction =
        Seq(("s1", "p1", "20170515", "t1", 5), ("s1", "p2", "20170515", "t2", 1), ("s1", "p3", "20170515", "t2", 1))
          .toDF(STORE_FIELD, PRODUCT_FIELD, DATE_FIELD, TRANSACTION_FIELD, QUANTITY_FIELD)
      val referential =
        Seq(("s1", "p1", 10.00), ("s1", "p2", 100.05))
          .toDF(STORE_FIELD, PRODUCT_FIELD, PRICE_FIELD)
      val dataFrame = transaction.addPrice(referential)
      dataFrame.where(s"$STORE_FIELD ='s1' AND $PRODUCT_FIELD='p1'").head.getAs[Double](TOTAL_PRICE_FIELD) should equal(50.0)
      dataFrame.where(s"$STORE_FIELD ='s1' AND $PRODUCT_FIELD='p2'").head.getAs[Int](TOTAL_PRICE_FIELD) should equal(100.05)
      dataFrame.count should equal(2)
    }
  }

  feature("topColumnWindows function ") {
    scenario("ALL_OK") {
      val transaction =
        Seq(("s1", "p1", "20170515", "t1", 5, 100.5),
            ("s1", "p2", "20170515", "t2", 1, 101.5),
            ("s1", "p2", "20170515", "t2", 0, 80.5),
            ("s2", "p3", "20170515", "t2", 1, 5.0))
          .toDF(STORE_FIELD, PRODUCT_FIELD, DATE_FIELD, TRANSACTION_FIELD, QUANTITY_FIELD, TOTAL_PRICE_FIELD)

      val dataFrameByQuantity = transaction.topColumnWindows(Set(STORE_FIELD), QUANTITY_FIELD, 2)
      val dataFrameByPrice    = transaction.topColumnWindows(Set(STORE_FIELD), TOTAL_PRICE_FIELD, 2)
      dataFrameByPrice.where(s"$STORE_FIELD ='s1' ").head.getAs[Double](TOTAL_PRICE_FIELD) should equal(101.5)
      dataFrameByQuantity.where(s"$STORE_FIELD ='s1' ").head.getAs[Double](QUANTITY_FIELD) should equal(5)
      dataFrameByPrice.where(s"$STORE_FIELD ='s1' ").count should equal(2)
      dataFrameByPrice.where(s"$STORE_FIELD ='s2' ").count should equal(1)
    }
  }

  feature("topColumn function ") {
    scenario("ALL_OK") {
      val transaction =
        Seq(
          ("s1", "p1", "20170515", "t1", 5, 100.5),
          ("s1", "p2", "20170515", "t2", 1, 101.5),
          ("s1", "p2", "20170515", "t2", 0, 80.5),
          ("s2", "p3", "20170515", "t2", 1, 5.0),
          ("s3", "p3", "20170515", "t2", 1, 5.0)
        ).toDF(STORE_FIELD, PRODUCT_FIELD, DATE_FIELD, TRANSACTION_FIELD, QUANTITY_FIELD, TOTAL_PRICE_FIELD)

      val dataFrameByQuantity = transaction.topColumn(Set(STORE_FIELD), QUANTITY_FIELD, 2)
      val dataFrameByPrice    = transaction.topColumn(Set(STORE_FIELD), TOTAL_PRICE_FIELD, 2)
      dataFrameByPrice.where(s"$STORE_FIELD ='s1' ").head.getAs[Double](TOTAL_PRICE_FIELD) should equal(282.5)
      dataFrameByQuantity.where(s"$STORE_FIELD ='s1' ").head.getAs[Double](QUANTITY_FIELD) should equal(6)
      dataFrameByPrice.count should equal(2)
    }
  }

  feature("aggregate function ") {
    scenario("ALL_OK") {

      val transaction =
        Seq(
          ("s1", "p1", "20170515", "t1", 5, 100.5),
          ("s1", "p2", "20170515", "t2", 1, 101.5),
          ("s1", "p2", "20170515", "t2", 0, 80.5),
          ("s2", "p3", "20170515", "t2", 1, 5.0),
          ("s3", "p3", "20170515", "t2", 1, 5.0)
        ).toDF(STORE_FIELD, PRODUCT_FIELD, DATE_FIELD, TRANSACTION_FIELD, QUANTITY_FIELD, TOTAL_PRICE_FIELD)

      val dataFrameByQuantity = transaction.aggregate(Set(STORE_FIELD), QUANTITY_FIELD)
      val dataFrameByPrice    = transaction.aggregate(Set(STORE_FIELD), TOTAL_PRICE_FIELD)
      dataFrameByPrice.where(s"$STORE_FIELD ='s1' ").head.getAs[Double](TOTAL_PRICE_FIELD) should equal(282.5)
      dataFrameByQuantity.where(s"$STORE_FIELD ='s1' ").head.getAs[Double](QUANTITY_FIELD) should equal(6)
      dataFrameByPrice.count should equal(3)
    }
  }

  feature("checkDateConsistency function ") {
    scenario("ALL OK ") {

      val transaction =
        Seq(
          ("s1", "p1", "20170615", "t1", 5, 100.5),
          ("s1", "p2", "20170615", "t2", 1, 101.5),
          ("s1", "p2", "20170615", "t2", 0, 80.5)
        ).toDF(STORE_FIELD, PRODUCT_FIELD, DATE_FIELD, TRANSACTION_FIELD, QUANTITY_FIELD, TOTAL_PRICE_FIELD)

      transaction.checkDateConsistency("20170615") should equal(())
    }

    scenario("Multiple date ") {

      val transaction =
        Seq(
          ("s1", "p1", "20170615", "t1", 5, 100.5),
          ("s1", "p2", "20170515", "t2", 1, 101.5),
          ("s1", "p2", "20170615", "t2", 0, 80.5)
        ).toDF(STORE_FIELD, PRODUCT_FIELD, DATE_FIELD, TRANSACTION_FIELD, QUANTITY_FIELD, TOTAL_PRICE_FIELD)
      the[RuntimeException] thrownBy transaction.checkDateConsistency("20170615")
    }

    scenario("different date ") {

      val transaction =
        Seq(
          ("s1", "p1", "20170615", "t1", 5, 100.5),
          ("s1", "p2", "20170615", "t2", 1, 101.5),
          ("s1", "p2", "20170615", "t2", 0, 80.5)
        ).toDF(STORE_FIELD, PRODUCT_FIELD, DATE_FIELD, TRANSACTION_FIELD, QUANTITY_FIELD, TOTAL_PRICE_FIELD)
      the[RuntimeException] thrownBy transaction.checkDateConsistency("20170515")

    }
  }
  /*







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
 */
}
