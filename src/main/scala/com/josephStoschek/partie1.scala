package com.josephStoschek

import java.nio.file.FileSystem

import java.text.SimpleDateFormat

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}



object partie1 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    import org.apache.spark.sql.functions._

    // Question 1 : Lire tous ces fichiers sous forme d'un `seul` Dataframe (ou d'un Dataset, au choix).
    /*val df:DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("C:\\Users\\stosc\\Documents\\ESME\\Ingé3_2020-2021\\FrameworkBigData\\TP2_note\\src\\main\\scala\\com\\josephStoschek\\data\\*.csv")

    val df_rename = df.withColumnRenamed("_c0", "dataframe")
    df_rename.show(30)*/

    import sparkSession.implicits._
    var df1: DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("C:\\Users\\stosc\\Documents\\ESME\\Ingé3_2020-2021\\FrameworkBigData\\TP2_note\\src\\main\\scala\\com\\josephStoschek\\data\\02112020.csv")
    var df1_count = df1.count()
    while (df1_count <8) {
      df1 = df1.union(Seq(("null")).toDF)
      df1_count = df1_count + 1
    }
    //df1.show()

    var df2: DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("C:\\Users\\stosc\\Documents\\ESME\\Ingé3_2020-2021\\FrameworkBigData\\TP2_note\\src\\main\\scala\\com\\josephStoschek\\data\\03112020.csv")
    var df2_count = df2.count()
    while (df2_count <8) {
      df2 = df2.union(Seq(("null")).toDF)
      df2_count = df2_count + 1
    }
    //df2.show()

    var df3: DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("C:\\Users\\stosc\\Documents\\ESME\\Ingé3_2020-2021\\FrameworkBigData\\TP2_note\\src\\main\\scala\\com\\josephStoschek\\data\\04112020.csv")
    var df3_count = df3.count()
    while (df3_count <8) {
      df3 = df3.union(Seq(("null")).toDF)
      df3_count = df3_count + 1
    }
    //df3.show()

    var df4: DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("C:\\Users\\stosc\\Documents\\ESME\\Ingé3_2020-2021\\FrameworkBigData\\TP2_note\\src\\main\\scala\\com\\josephStoschek\\data\\05112020.csv")
    var df4_count = df4.count()
    while (df4_count <8) {
      df4 = df4.union(Seq(("null")).toDF)
      df4_count = df4_count + 1
    }
    //df4.show()

    var df5: DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("C:\\Users\\stosc\\Documents\\ESME\\Ingé3_2020-2021\\FrameworkBigData\\TP2_note\\src\\main\\scala\\com\\josephStoschek\\data\\06112020.csv")
    var df5_count = df5.count()
    while (df5_count <8) {
      df5 = df5.union(Seq(("null")).toDF)
      df5_count = df5_count + 1
    }
    //df5.show()

    var df6: DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("C:\\Users\\stosc\\Documents\\ESME\\Ingé3_2020-2021\\FrameworkBigData\\TP2_note\\src\\main\\scala\\com\\josephStoschek\\data\\07112020.csv")
    var df6_count = df6.count()
    while (df6_count < 8) {
      df6 = df6.union(Seq(("null")).toDF)
      df6_count = df6_count + 1
    }
    //df6.show()

    val df7 = df1.union(df2)
    val df8 = df7.union(df3)
    val df9 = df8.union(df4)
    val df10 = df9.union(df5)
    val df_final = df10.union(df6)

    //df_final.show(48)



    //Question 2 : Rajouter une colonne contenant la date du fichier duquel vient chacune des lignes (y'a une fonction dans Spark qui vous le récupère depuis le nom du fichier directement, c'est cadeau).
    sparkSession.udf.register(name = "get_file_name", (path: String) => path.split("/").last.split("\\.").head)
    val df_add_col = df_final.withColumn(colName = "file_date", callUDF(udfName = "get_file_name", input_file_name))
    //df_add_col.show()


    //Question 3 :Remplir les gaps comme décrit précédemment.

    // Question 4 : Écrire les résultats en partitionnant par date (la colonne que nous avons rajouté dans la question 2.)

    df_add_col.write.partitionBy("file_date").mode(SaveMode.Overwrite).parquet("resultat")
    val parquetDF = sparkSession.read.parquet("resultat")
    parquetDF.show()

    }


}
