package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ProjectName: SparkCore 
 * @Package:
 * @ClassName: TransformationDemo2
 * @Author: Administrator
 * @Description: ${description}
 * @Date:    2020/3/17 6:19
 * @Version:    1.0
*/object TransformationDemo2 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("TransformationDemo2")
    val sc = new SparkContext(conf)
    //并集union
    val rdd1: RDD[Int] = sc.parallelize(List(5, 6, 7, 8))
    val rdd2: RDD[Int] = sc.parallelize(List(1, 2, 5, 6))
    val rdd3: RDD[Int] = rdd1 union rdd2
    println(rdd3.collect().toBuffer)
    println("====================")
    //交集 intersection
    val rdd4: RDD[Int] = rdd1.intersection(rdd2)
    println(rdd4.collect().toBuffer)
    //去重 distinct
    val rdd5: RDD[Int] = rdd3.distinct()
    println(rdd5.collect().toBuffer)
    //join 对相同的key进行合并
    val rdd6: RDD[(String, Int)] = sc.parallelize(List(("tom", 1), ("jerry", 3), ("ketty", 2)))
    val rdd7: RDD[(String, Int)] = sc.parallelize(List(("jerry", 1), ("tom", 2), ("tom", 10)))
    val rdd9: RDD[(String, Int)] = sc.parallelize(List(("jerry", 5), ("tom", 6), ("tom", 7)))
    val rdd99: RDD[(String, (Int, Int))] = rdd6 join rdd7
    val rdd8: RDD[(String, ((Int, Int), Int))] = rdd6 join rdd7 join rdd9
    println(rdd99.collect().toBuffer)
    println(rdd8.collect().toBuffer)
  }

}
