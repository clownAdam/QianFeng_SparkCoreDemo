package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ProjectName: SparkCore 
 * @Package:
 * @ClassName: TransformationDemo6
 * @Author: Administrator
 * @Description: ${description}
 * @Date:    2020/3/17 23:31
 * @Version:    1.0
*/
object TransformationDemo6 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("TransformationDemo6")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7))
    val sum: Int = rdd.reduce(_ + _)
    println(sum)
  /*
   * reduceByKey
   */
    val value: RDD[(String, Int)] = sc.parallelize(List(("cat", 2), ("cat", 5), ("pig", 10), ("dog", 3), ("dog", 4)))
    val value1: RDD[(String, Int)] = value.reduceByKey(_ + _)
    val value2: RDD[(String, Int)] = value1.reduceByKey(_ + _)
    println(value2.collect().toList)
    /* a*/
    val value3: RDD[(String, Int)] = value.aggregateByKey(1)(_ + _, _ + _)
    println(value3.collect().toList)

    val sum4: RDD[(String, Int)] = value.combineByKey(x => x, (a: Int, b: Int) => a + b, (m: Int, n: Int) => m + n)
    println(sum4.collect().toList)
  }
}
