package com.carrefour.phenix.service.impl

import java.io.File
import java.nio.file.Path

import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfterAll, FeatureSpec, Matchers}

class DefaultFileManagerTest extends FeatureSpec with BeforeAndAfterAll with Matchers {
  val sparkSession = SparkSession
    .builder()
    .master("local[2]")
    .getOrCreate()
  import sparkSession.implicits._

  val INPUT_PATH         = "src/test/resources/input"
  val DATE               = "20190423"
  val defaultFileManager = new DefaultFileManager(sparkSession, 2)
  feature("getFileAbsolutePathByStoreId ") {
    scenario("ALL_OK") {
      val files = defaultFileManager.getFileAbsolutePathByStoreId(s"$INPUT_PATH", DATE)
      files.map(value => { (value._1, new File(value._2).getName) }) should equal(
        Map("s1" -> "reference_prod-s1_20190423.data", "s2" -> "reference_prod-s2_20190423.data"))
    }

    scenario(" path not found") {
      the[Exception] thrownBy defaultFileManager.getFileAbsolutePathByStoreId("toto", DATE)
    }
  }

  feature("loadCsvReferentialFiles ") {
    val files = Map("s1" -> s"$INPUT_PATH/referential/reference_prod-s1_20190423.data",
                    "s2" -> s"$INPUT_PATH/referential/reference_prod-s2_20190423.data")
    val dataFrame =
      Seq(("s1", "p1", 10), ("s1", "p2", 1), ("s1", "p3", 5), ("s1", "p4", 20), ("s2", "p1", 10), ("s2", "p2", 100), ("s2", "p3", 5))
        .toDF(STORE_FIELD, PRODUCT_FIELD, PRICE_FIELD)

    scenario("ALL_OK") {
      val df = defaultFileManager.loadCsvReferentialFiles(files)
      df.count() should equal(dataFrame.count)
    }
  }
}
