package com.mts.analytics.main

import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

import scala.collection.mutable

object Main {

  def show(x: Option[String]): String = x match {
    case Some(s) => s
    case None => "?"
  }

  def customMap(): mutable.Map[String, String] = {
    var mapMut = scala.collection.mutable.Map(
      "file1_f1" -> "0",
      "file1_f2" -> "NA",
      "file1_f3" -> "NA",
      "file2_f1" -> "0",
      "file2_f2" -> "NA",
      "file2_f3" -> "NA",
      "file3_f1" -> "0",
      "file3_f2" -> "NA",
      "file3_f3" -> "NA"
    )

    return mapMut;
  }

  def listToColumns(names: Set[String]): String = {

    var str = "";
    for (name <- names) {
      str = str + "'" + show(customMap().get(name)) + "' as " + name + " ,";
    }
    str = str.replaceAll(",$", "");
    println(str)

    return str
  }

  def emptyDataFrame(): DataFrame = {
    //
    val schema = StructType(
      //      StructField("AAP_WELCOME_TNO", DecimalType(38, 10), true) ::
      StructField("ENAME", StringType, true) ::
        StructField("JOB", StringType, true) ::
        StructField("MGR", DecimalType(38, 10), true) ::
        StructField("SAL", DecimalType(38, 10), true) ::
        StructField("COMM", DecimalType(38, 10), true) ::
        StructField("AAP_DEPT_TNO", DecimalType(38, 10), true) ::

        StructField("file1_f1", IntegerType, true) ::
        StructField("file1_f2", StringType, true) ::
        StructField("file1_f3", StringType, true) ::

        StructField("file2_f1", IntegerType, true) ::
        StructField("file2_f2", StringType, true) ::
        StructField("file2_f3", StringType, true) ::

        StructField("file3_f1", IntegerType, true) ::
        StructField("file3_f2", StringType, true) ::
        StructField("file3_f3", StringType, true) ::
        Nil
    )


    var dataRDD = getSparkSession().sparkContext.emptyRDD[Row]

    //pass rdd and schema to create dataframe
    val newDFSchema = getSparkSession().createDataFrame(dataRDD, schema)


    newDFSchema.createOrReplaceTempView("tempSchema")

    val df = getSparkSession().sql("select * from tempSchema")
    return df;
  }

  def getSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder
      .appName("spark")
      .master("local[*]")
      .getOrCreate()
    return spark;
  }



  def writeOperation(dataFrame: DataFrame): Unit = {


    dataFrame.createOrReplaceTempView("welcome_tbl")
    //    //print(dataFrame)
    //    println(emptyDataFrame().columns)
    //    println(dataFrame.columns)
    //    val emptyColumns = emptyDataFrame().columns.mkString(",").split(",")
    //    val mainColumns = dataFrame.columns.mkString(",").split(",")
    val list1 = emptyDataFrame().columns.toSet
    val list2 = dataFrame.columns.toSet
    val newList = list1.filterNot(list2);
    var res = listToColumns(newList);
    println("res ==> " + res)
    var mainColumnsString = dataFrame.columns.mkString(",")
    val finalDataFrameValues = " SELECT  " + mainColumnsString + ", " + res + " FROM welcome_tbl";
    println("finalDataFrameValues =>" + finalDataFrameValues)
    val df3 = getSparkSession().sql(finalDataFrameValues);
    df3.printSchema()
    df3.show(10)
    //println(newList.mkString(","))


    //    println("Empty Column Names : "+emptyColumns)
    //    println("mainColumns Column Names : "+mainColumns)

    df3.write.mode(SaveMode.Append).format("jdbc")
      .option("url", "jdbc:oracle:thin:@localhost:1521:xe")
      .option("dbtable", "AAP_WELCOME_GENERIC")
      .option("user", "sys as sysdba")
      .option("password", "welcome")
      .option("driver", "oracle.jdbc.driver.OracleDriver")
      .save()

  }

  def source_file1(): DataFrame = {
    val df = getSparkSession().read.format("csv").option("header", "true").load("C:\\data\\spark_output\\file1\\file1_welcome.csv");
    return df;
  }

  def source_file2(): DataFrame = {
    val df = getSparkSession().read.format("csv").option("header", "true").load("C:\\data\\spark_output\\file2\\file2_welcome.csv");
    return df;
  }

  def source_file3(): DataFrame = {
    val df = getSparkSession().read.format("csv").option("header", "true").load("C:\\data\\spark_output\\file3\\file3_welcome.csv");
    return df;
  }

  def main(args: Array[String]): Unit = {
    //    val inputDataFrame=source_file1();
    //     val inputDataFrame=source_file2();
    val inputDataFrame = source_file3();
    writeOperation(inputDataFrame)
  }

}
