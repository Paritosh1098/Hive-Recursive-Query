package edu.wpi.big.data.management

import edu.wpi.big.data.management.utils.SparkUtil
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}

object SparkTest {

  def main(args: Array[String]): Unit = {
    SparkUtil.conf_=(new SparkConf().setAppName("SparkTest").setMaster("local[2]"))

    val data: Array[Row] = Array(Row(1, 2), Row(2, 3), Row(3, 4), Row(4, 5))
    val rdd: RDD[Row] = SparkUtil.sparkContext.parallelize(data, 2)
    val schema: StructType = StructType(
      Seq(StructField("a", IntegerType, false), StructField("b", IntegerType, false))
    )
    val dataFrame: DataFrame = SparkUtil.sqlContext.createDataFrame(rdd, schema)

    dataFrame.show()

    dataFrame.toDF(Seq("x", "y"): _*).show()
  }
}
