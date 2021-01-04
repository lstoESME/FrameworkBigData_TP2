package com.josephStoschek

import java.nio.file.FileSystem

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, SparkSession}


object partie1 {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.OFF)
    val sparkSession = SparkSession.builder().master("local").getOrCreate()

    import org.apache.spark.sql.functions._

    // Question 1 : Lire tous ces fichiers sous forme d'un `seul` Dataframe (ou d'un Dataset, au choix).
    /*val df:DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("C:\\Users\\stosc\\Documents\\ESME\\Ingé3_2020-2021\\FrameworkBigData\\TP2_note\\src\\main\\scala\\com\\josephStoschek\\data\\*.csv")

    val df_rename = df.withColumnRenamed("_c0", "dataframe")
    df_rename.show(30)*/

    //Question 2 : Rajouter une colonne contenant la date du fichier duquel vient chacune des lignes (y'a une fonction dans Spark qui vous le récupère depuis le nom du fichier directement, c'est cadeau).
    import sparkSession.implicits._
    val df1: DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("C:\\Users\\stosc\\Documents\\ESME\\Ingé3_2020-2021\\FrameworkBigData\\TP2_note\\src\\main\\scala\\com\\josephStoschek\\data\\02112020.csv")
    val df1_count = df1.count()
    while (df1_count <8) {
      val new_row= Seq(("null")).toDF("dataframe")
      val new_df1 = df1.union(new_row)
      println(new_df1)
    }
    //println(df1)
    val df2: DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("C:\\Users\\stosc\\Documents\\ESME\\Ingé3_2020-2021\\FrameworkBigData\\TP2_note\\src\\main\\scala\\com\\josephStoschek\\data\\03112020.csv")
    val df2_count = df2.count()
    while (df2_count <8) {
      val new_row= Seq(("null")).toDF("dataframe")
      val new_df2 = df2.union(new_row)
      println(new_df2)
    }

    /*val df3: DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("C:\\Users\\stosc\\Documents\\ESME\\Ingé3_2020-2021\\FrameworkBigData\\TP2_note\\src\\main\\scala\\com\\josephStoschek\\data\\04112020.csv")
    val df4: DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("C:\\Users\\stosc\\Documents\\ESME\\Ingé3_2020-2021\\FrameworkBigData\\TP2_note\\src\\main\\scala\\com\\josephStoschek\\data\\05112020.csv")
    val df5: DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("C:\\Users\\stosc\\Documents\\ESME\\Ingé3_2020-2021\\FrameworkBigData\\TP2_note\\src\\main\\scala\\com\\josephStoschek\\data\\06112020.csv")
    val df6: DataFrame = sparkSession.read.option("header", false).option("delimiter", ";").option("inferSchema", true).csv("C:\\Users\\stosc\\Documents\\ESME\\Ingé3_2020-2021\\FrameworkBigData\\TP2_note\\src\\main\\scala\\com\\josephStoschek\\data\\07112020.csv")
    val df7 = df1.union(df2)
    val df8 = df7.union(df3)
    val df9 = df8.union(df4)
    val df10 = df9.union(df5)
    val df_final = df10.union(df6) */

  }
}
