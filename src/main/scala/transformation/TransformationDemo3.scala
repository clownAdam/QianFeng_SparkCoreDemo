package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ProjectName: SparkCore 
 * @Package:
 * @ClassName: TransformationDemo3
 * @Author: Administrator
 * @Description: ${description}
 * @Date:    2020/3/17 6:44
 * @Version:    1.0
*/
object TransformationDemo3 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("TransformationDemo3")
    val sc = new SparkContext(conf)
    //左连接：以左边rdd为基准
    val rdd1: RDD[(String, Int)] = sc.parallelize(List(("tom", 1), ("jerry", 3), ("ketty", 2)))
    val rdd2: RDD[(String, Int)] = sc.parallelize(List(("jerry", 1), ("tom", 2), ("tom", 10)))
    val rdd3: RDD[(String, (Int, Option[Int]))] = rdd1 leftOuterJoin rdd2
//    println(rdd3.collect().toBuffer)
    //有链接
    val rdd4: RDD[(String, (Option[Int], Int))] = rdd1 rightOuterJoin (rdd2)
//    println(rdd4.collect().toBuffer)
    //笛卡尔积 cartesian
//    println(rdd1.cartesian(rdd2).collect().toBuffer)
//    println(rdd2.cartesian(rdd1).collect().toBuffer)
    //groupby 分组，根据传入的参数进行分组
    val rdd6: RDD[(String, Int)] = sc.parallelize(List(("tom", 1), ("jerry", 3), ("ketty", 2), ("jerry", 1), ("tom", 2), ("tom", 10)))
    val rdd7: RDD[(Int, Iterable[(String, Int)])] = rdd6.groupBy(_._2)
    println(rdd7.collect().toBuffer)
  }

}
