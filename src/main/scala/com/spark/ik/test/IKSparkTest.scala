package com.spark.ik.test
import org.apache.spark.SparkContext
object IKSparkTest {
  
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext("local[2]", "WordCount", System.getenv("SPARK_HOME"))
    
    /*val reg = sc.textFile("D:/Workspace/scala/SparkCSVJoin/ik/acc_trn_details.txt","D:/Workspace/scala/SparkCSVJoin/ik/acc_trn_details.txt").map(_.split(","))*/
    
    val r1 = sc.textFile("D:/Workspace/scala/SparkCSVJoin/ik/acc_trn_details.txt")
    val r2 = sc.textFile("D:/Workspace/scala/SparkCSVJoin/ik/bank_schema_details.txt")
    val rdds = Seq(r1, r2)
    val bigRdd = sc.union(rdds)
    
    val mr = bigRdd.map(_.split(" ")).map { x => /*println("Data"+x.size)*/
          x.size match {
            case 6 => println("Size is matched"+ x(0),x(1),x(2),x(3))
            case 4 => println("Size is matched"+ x(0),x(1),x(2))
            case _ => println("Data not matched"+x.size)
          }
    
    }
    
    mr.foreach { println }
     
  
  }
}