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
object ActionDemo {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("ActionDemo")
    val sc = new SparkContext(conf)
    val rdd1: RDD[Int] = sc.parallelize(List(2, 1, 3, 6, 5))
    //collect:将rdd转换为集合或数组的形式并展示所有数据
    println(rdd1.collect().toList)
    //count算子：可以返回rdd中存储元素的个数
    println(rdd1.count())
    //top算子，可以反序取出存储在rdd中的元素，根据传入的数值取出对应的个数,自带默认的降序排序
    val array: Array[Int] = rdd1.top(3)
    println(array.toBuffer)
    //take算子：顺序取出数据，没有排序
    val ints: Array[Int] = rdd1.take(3)
    println(ints.toBuffer)
    val ints1: Array[Int] = rdd1.takeOrdered(5)
    println(ints1.toBuffer)
    println(rdd1.first())
    val unit: Unit = rdd1.foreach(println)

    //    rdd1.saveAsTextFile("output002")
    sc.stop()
  }
}
