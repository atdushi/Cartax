package org.example

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object App {
  def main(args: Array[String]): Unit = {
    val spark = init()

    var df = extract(spark, args.apply(0))

    df.printSchema()

    df = cleansing(df)

    df = enrich(df)

    df = load(df, args.apply(1));

    df.show()
  }

  private def init(): SparkSession = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("Task5")
      .getOrCreate()

    // чтобы сильно не флудил
    //    spark.sparkContext.setLogLevel("ERROR")

    // для парсинга даты
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

    spark
  }

  private def extract(spark: SparkSession, path: String): DataFrame = {
    val schema =
      StructType(
        Array(
          StructField("VendorID", IntegerType, nullable = false),
          StructField("tpep_pickup_datetime", StringType, nullable = false),
          StructField("tpep_dropoff_datetime", StringType, nullable = false),
          StructField("passenger_count", IntegerType, nullable = false),
          StructField("trip_distance", FloatType, nullable = false),
          StructField("RatecodeID", IntegerType, nullable = false),
          StructField("store_and_fwd_flag", StringType),
          StructField("PULocationID", IntegerType),
          StructField("DOLocationID", IntegerType),
          StructField("payment_type", IntegerType, nullable = false),
          StructField("fare_amount", FloatType, nullable = false),
          StructField("extra", FloatType, nullable = false),
          StructField("mta_tax", FloatType, nullable = false),
          StructField("tip_amount", FloatType, nullable = false),
          StructField("tolls_amount", FloatType, nullable = false),
          StructField("improvement_surcharge", FloatType, nullable = false),
          StructField("total_amount", FloatType, nullable = false),
          StructField("congestion_surcharge", FloatType, nullable = false)
        )
      )

    val df = spark.read
      .option("header", true)
      .schema(schema)
      .csv(path)

    df
  }

  private def cleansing(df: DataFrame): DataFrame = {
    // удалим отрицательную стоимость поездки
    // и неизвестное кол-ов пассажиров
    df
      .filter(col("total_amount") >= 0)
      .filter(col("passenger_count").isNotNull)
  }

  private def enrich(df: DataFrame): DataFrame = {
    // добавим общее число поездок за дату
    df.withColumn("date", to_date(col("tpep_pickup_datetime"), "yyyy-MM-dd"))
      .withColumn("trip_count", count(col("date")).over(Window.partitionBy("date")))
  }

  private def load(df: DataFrame, path: String): DataFrame = {
    // вспомогательный датасет сгруппированный по датам и кол-ву пассажиров
    val df_aux = df.groupBy("date", "passenger_count").agg(
      count("passenger_count").as("count"),
      first("trip_count").as("trip_count"),
      max("total_amount").as("max_total_amount"),
      min("total_amount").as("min_total_amount")
    )

    val df_0p = df_aux.filter(col("passenger_count") === 0)

    val df_1p = df_aux.filter(col("passenger_count") === 1)

    val df_2p = df_aux.filter(col("passenger_count") === 2)

    val df_3p = df_aux.filter(col("passenger_count") === 3)

    val df_4p_plus = df_aux
      .filter(col("passenger_count") >= 4)
      .groupBy(col("date"))
      .agg(
        sum("count").as("count"),
        first("trip_count").as("trip_count"),
        max("max_total_amount").as("max_total_amount"),
        min("min_total_amount").as("min_total_amount"),
      )

    val df_all = df_0p.alias("df0")
      .join(df_1p.alias("df1"),
        col("df0.date") === col("df1.date"), "outer")
      .join(df_2p.alias("df2"),
        coalesce(col("df0.date"), col("df1.date")) === col("df2.date"), "outer")
      .join(df_3p.alias("df3"),
        coalesce(col("df0.date"), col("df1.date"), col("df2.date")) === col("df3.date"), "outer")
      .join(df_4p_plus.alias("df4"),
        coalesce(col("df0.date"), col("df1.date"), col("df2.date"), col("df3.date")) === col("df4.date"), "outer")
      .select(
        coalesce(
          col("df0.date"),
          col("df1.date"),
          col("df2.date"),
          col("df3.date"),
          col("df4.date"))
          .alias("date"),
        coalesce(round(col("df0.count") / col("df0.trip_count") * 100), lit(0)).alias("percentage_zero").cast(IntegerType),
        coalesce(round(col("df1.count") / col("df1.trip_count") * 100), lit(0)).alias("percentage_1p").cast(IntegerType),
        coalesce(round(col("df2.count") / col("df2.trip_count") * 100), lit(0)).alias("percentage_2p").cast(IntegerType),
        coalesce(round(col("df3.count") / col("df3.trip_count") * 100), lit(0)).alias("percentage_3p").cast(IntegerType),
        coalesce(round(col("df4.count") / col("df4.trip_count") * 100), lit(0)).alias("percentage_4p_plus").cast(IntegerType),
        coalesce(col("df0.max_total_amount"), lit("0.0")).alias("zero_max_total_amount"),
        coalesce(col("df0.min_total_amount"), lit("0.0")).alias("zero_min_total_amount"),
        coalesce(col("df1.max_total_amount"), lit("0.0")).alias("1p_max_total_amount"),
        coalesce(col("df1.min_total_amount"), lit("0.0")).alias("1p_min_total_amount"),
        coalesce(col("df2.max_total_amount"), lit("0.0")).alias("2p_max_total_amount"),
        coalesce(col("df2.min_total_amount"), lit("0.0")).alias("2p_min_total_amount"),
        coalesce(col("df3.max_total_amount"), lit("0.0")).alias("3p_max_total_amount"),
        coalesce(col("df3.min_total_amount"), lit("0.0")).alias("3p_min_total_amount"),
        coalesce(col("df4.max_total_amount"), lit("0.0")).alias("4p_plus_max_total_amount"),
        coalesce(col("df4.min_total_amount"), lit("0.0")).alias("4p_plus_min_total_amount")
      )

    df_all.write.parquet(path)

    df_all
  }
}
