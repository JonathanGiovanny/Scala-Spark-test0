package com.cadreon.unity.importer

import org.apache.spark.sql.SparkSession

class Importer {

  def getFilePath(fileName: String): String = {
    "src\\main\\resources\\" + fileName
  }

  def getSparkSession(appName: String): SparkSession = SparkSession
    .builder
    .master("local[*]")
    .appName(appName)
    .getOrCreate()
}
