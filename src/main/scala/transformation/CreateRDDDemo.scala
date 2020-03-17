package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ProjectName: SparkCore 
 * @Package:
 * @ClassName: CreateRDDDemo
 * @Author: Administrator
 * @Description: ${description}
 * @Date:    2020/3/13 21:59
 * @Version:    1.0
*/object CreateRDDDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("CreateRDDDemo").setMaster("local")
    val sc = new SparkContext(conf)
    val value: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4, 5, 6, 7))
    val value1: RDD[Int] = sc.makeRDD(Array(1, 2, 3, 4))
    val value2: RDD[Nothing] = sc.parallelize(List())
    val value3: RDD[String] = sc.textFile("input")
    val count: Long = value3.count()
    val first: String = value3.first()
    val array: Array[String] = value3.take(1)

    println(array.mkString)
    println(first)
    println(count)
  }

}
