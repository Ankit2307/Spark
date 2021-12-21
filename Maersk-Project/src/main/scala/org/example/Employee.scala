package org.example

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Employee {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Employee_Vaccination").master("local[1]").getOrCreate()
    spark.conf.set("spark.sql.shuffle.partitions", 5)
    val readDF = spark.read.option("header", true).csv("src/main/resources/us-500.csv")
    // Replicate 100X more data points.
    val replicateDataDF = readDF.withColumn("demo", explode(array((1 to 100).map(lit): _*)))
      .selectExpr(readDF.columns: _*).drop("demo")
    val repartitionDF = replicateDataDF.repartition(5)
    // printing the count of data on the console to meet the requirement of 100x replication.
    println("Count of the data after Replication " + replicateDataDF.count())
    val transformDF = repartitionDF.groupBy("city").count().orderBy(desc("count"))
      .select(col("city").alias("transform_city"), col("count").alias("Density"))
    // join to merge the two dataframe to contains all the data.
    val CityEmployeeDensityDF = repartitionDF
      .join(transformDF,
        repartitionDF.col("city") === transformDF.col("transform_city"), "inner").drop("transform_city")
    val winspec = Window.partitionBy("state").orderBy(desc("Density"))
    val winspec1 = Window.partitionBy("city").orderBy(asc("zip"))
    val winspec2 = Window.partitionBy("city")
    val CityEmployeeDensityTransformDF = CityEmployeeDensityDF.withColumn("Employee_Row", row_number().over(winspec1))
      .withColumn("Sequence", dense_rank().over(winspec))
    CityEmployeeDensityTransformDF.show(25)
    val VaccinationDrivePlanDF = CityEmployeeDensityTransformDF
      .withColumn("Vaccination_Start_Date", when(col("Employee_Row").between(1, 100), lit(current_date()))
        .when(col("Employee_Row").between(100, 200), lit(date_add(current_date(), 1)))
        .when(col("Employee_Row").between(200, 300), lit(date_add(current_date(), 2)))
        .when(col("Employee_Row").between(300, 400), lit(date_add(current_date(), 3)))
        .when(col("Employee_Row").between(400, 500), lit(date_add(current_date(), 4)))
        .when(col("Employee_Row").between(500, 600), lit(date_add(current_date(), 5)))
        .when(col("Employee_Row").between(600, 700), lit(date_add(current_date(), 6)))
        .when(col("Employee_Row").between(700, 800), lit(date_add(current_date(), 7)))
        .when(col("Employee_Row").between(800, 900), lit(date_add(current_date(), 8)))
        .when(col("Employee_Row").between(900, 1000), lit(date_add(current_date(), 9)))
        .when(col("Employee_Row").between(1000, 1100), lit(date_add(current_date(), 10)))
        .when(col("Employee_Row").between(1100, 1200), lit(date_add(current_date(), 11)))
        .when(col("Employee_Row").between(1200, 1300), lit(date_add(current_date(), 12)))
        .when(col("Employee_Row").between(1300, 1400), lit(date_add(current_date(), 13))))
      .withColumn("Vaccination_End_Date", lit(max("Vaccination_Start_Date")).over(winspec2))
      .withColumn("Vaccination-drive-Completion-Time", datediff(col("Vaccination_End_Date"), col("Vaccination_Start_Date")))
    VaccinationDrivePlanDF.show(25)
  }
}
