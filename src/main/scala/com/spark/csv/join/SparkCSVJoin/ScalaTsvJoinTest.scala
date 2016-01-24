package com.spark.csv.join.SparkCSVJoin

import org.apache.spark.SparkContext
object ScalaTsvJoinTest {

  def main(args: Array[String]): Unit = {

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

    case class Register(d: java.util.Date, uuid: String, cust_id: String, lat: Float, lng: Float) {
      
      
      
      def returndata(x : String) : Int = {
        if(cust_id.isEmpty())
              None
        else 
          if (cust_id.trim().toInt > 4)
        return cust_id.trim().toInt
      
        return 0
      }
      
    }
    
    case class Click(d: java.util.Date, uuid: String, landing_page: Int)

      val sc = new SparkContext("local[2]", "WordCount", System.getenv("SPARK_HOME"))
    
    val reg = sc.textFile("D:/Workspace/scala/SparkCSVJoin/input/reg.tsv").map(_.split("\t")).map(
      r => (r(1), Register(format.parse(r(0)), r(1), r(2), r(3).toFloat, r(4).toFloat)))

    val clk = sc.textFile("D:/Workspace/scala/SparkCSVJoin/input/clk.tsv").map(_.split("\t")).map(
      c => (c(1), Click(format.parse(c(0)), c(1), c(2).trim.toInt)))

    val jonRDD = reg.join(clk)
    
    jonRDD.foreach(println)
  }
}