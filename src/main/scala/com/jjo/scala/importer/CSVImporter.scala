package com.cadreon.unity.importer

import com.cadreon.unity.model.{Contact, SF}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object CSVImporter extends Importer {

    def main(args: Array[String]): Unit = {
        val file: String = "StorefrontContact_Bulk Upload File_20190805.csv"
        val spark: SparkSession = getSparkSession("SF Importer")
        import spark.sqlContext.implicits._
        val storefronts: DataFrame = readFile(spark, file)
        val insertClauses: Dataset[String] = formatDataFrame(spark, storefronts)
        insertClauses.select("*").filter($"value".rlike("O\'K")).show(20, truncate = false)
        writeFile(insertClauses, "inserts")
        spark.stop
    }

    def readFile(sparkSession: SparkSession, filePath: String): DataFrame = sparkSession
      .read
      .option(key = "header", value = "true")
      .option(key = "encoding", value = "UTF8")
      .option(key = "sep", value = ";")
      .option(key = "inferSchema", value = "true")
      .csv(getFilePath(filePath))

    def writeFile(lines: Dataset[String], filePath: String) : Unit = {
        lines
          .write
          .text(getFilePath(filePath))
    }

    def generateInsert(sf: SF): String =
        "update sf set contacts = \'" + caseClassToJSON(sf.contacts) + "\' where id = " + sf.id + ";"

    def caseClassToJSON(contacts: List[Contact]): String = {
        import org.json4s._
        import org.json4s.JsonDSL._
        import org.json4s.jackson.JsonMethods._

        implicit val formats: DefaultFormats.type = DefaultFormats

        val json = contacts.map { c =>
            ("id" -> c.id) ~
            ("name" -> c.name) ~
              ("email" -> c.email) ~
              ("phoneNumber" -> c.phone) ~
              ("title" -> c.title) ~
              ("region" -> c.region)
        }

        compact(render(json)).replaceAll("\'", "\\\\'")
    }

    def formatDataFrame(sparkSession: SparkSession, dataFrame: DataFrame): Dataset[String] = {
        import sparkSession.sqlContext.implicits._
        dataFrame
          .select("*")
          .map(formatStoreFront)
          .filter(sf => sf.id != null)
          .map(generateInsert)
    }

    def formatStoreFront(row: Row): SF = {
        @scala.annotation.tailrec
        def loop(currentContacts: List[Contact], contactNumber: Int): List[Contact] = {
            if (contactNumber > 6) {
                currentContacts
            } else {
                loop(formatContacts(row, contactNumber) :: currentContacts, contactNumber + 1)
            }
        }
        SF(
            name = row.getString(0),
            id = getStoreFrontId(row.getString(1)),
            market = row.getString(2),
            loop(List[Contact](), 1)
              .filter(hasNoContactData)
              .reverse
        )
    }

    def getStoreFrontId(url: String): String =
        if (url != null) {
            url.replace("https://unity.cadreon.com/marketplace/#/marketplace/partner/", "").trim
        } else {
            url
        }

    def hasNoContactData(contact: Contact): Boolean =
        !(contact.name == null &&
          contact.email == null &&
          contact.region == null &&
          contact.title == null &&
          contact.phone == null)

    def formatContacts(row: Row, contactNumber: Int): Contact =
        if (contactNumber < 5) {
            formatFullContacts(row, contactNumber)
        } else {
            formatPartialContacts(row, contactNumber)
        }

    def formatFullContacts(row: Row, contactNumber: Int): Contact = Contact(
        name = getValueByColumnName(row, contactNumber, "Name"),
        email = getValueByColumnName(row, contactNumber, "Email"),
        region = getValueByColumnName(row, contactNumber, "Region"),
        title = getValueByColumnName(row, contactNumber, "Title"),
        phone = getValueByColumnName(row, contactNumber, "Phone")
    )

    def formatPartialContacts(row: Row, contactNumber: Int): Contact = Contact(
        name = getValueByColumnName(row, contactNumber, "Name"),
        email = getValueByColumnName(row, contactNumber, "Email"),
        phone = getValueByColumnName(row, contactNumber, "Phone"),
    )

    def getValueByColumnName(row: Row, iteration: Int, columnName: String): String = {
        val cell: String = row.getAs[String](s"Contact $iteration $columnName")
        if (cell != null) {
            cleanCellGarbage(cell)
        } else {
            cell
        }
    }

    def cleanCellGarbage(cell: String): String = {
        cell.replaceAll("\"\"", "")
            .replaceFirst("\"", "")
            .replaceAll("‹¯¨", "")
            .trim
    }
}