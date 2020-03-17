package action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ProjectName: SparkCore 
 * @Package: action
 * @ClassName: ActionDemo
 * @Author: Administrator
 * @Description: ${description}  
 * @Date:    2020/3/18 0:35
 * @Version:    1.0
*/
object ActionDemo2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ActionDemo2")
    val sc = new SparkContext(conf)
    val rdd: RDD[(String, Int)] = sc.parallelize(List(("c", 5), ("c", 3),("c", 3), ("d", 4), ("c", 2), ("a", 1)))
    //countByKey
    println(rdd.countByValue())
    val keySum: collection.Map[String, Long] = rdd.countByKey()
    println(keySum)
    val feef: RDD[(String, Int)] = rdd.filterByRange("2","3")
    println(rdd.collect().toList)
    //flatmapvalues
    val rdd2: RDD[(String, String)] = sc.parallelize(List(("a", "1 2"), ("b", "3 4")))
    println(rdd2.flatMap(_._2.split(" ")).collect().toList)
    val rdd2_1: RDD[(String, String)] = rdd2.flatMapValues(_.split(" "))
    println(rdd2_1.collect().toList)
//    rdd2.flatMapValues()
    sc.stop()
  }
}
