package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ProjectName: SparkCore 
 * @Package:
 * @ClassName: TransformationDemo
 * @Author: Administrator
 * @Description: ${description}
 * @Date:    2020/3/13 23:56
 * @Version:    1.0
*/
object TransformationDemo {
  def main(args: Array[String]): Unit = {
    //map算子
    val conf = new SparkConf().setAppName("TransformationDemo").setMaster("local")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    val rdd2: RDD[Int] = rdd.map(_*2)
    val res: Array[Int] = rdd2.collect()
    println(res.toBuffer)
    println(rdd.map(math.pow(_,2)).collect().toBuffer)
    println("+++++++++++++++++++")
    //filter算子
    val rdd3: RDD[Int] = rdd2.filter(_ > 10)
    println("hello")
    println(rdd3.collect().toBuffer)
    println("+++++++++++++++++++")
    rdd2.filter(_==1)
    //flatMap 是对rdd中的数据进行压平处理
    val rdd4: RDD[String] = sc.parallelize(Array("a b c", "b c d"))
    val rdd5: RDD[Array[String]] = rdd4.map(_.split(" "))
    val value: RDD[String] = rdd4.flatMap(_.split(" "))
    println(value.collect().toBuffer)

    println("===================================")
    //sample 随机抽样
    val rdd6: RDD[Int] = sc.parallelize(1 to 10)
    val rdd7: RDD[Int] = rdd6.sample(false, 0.5)
    println(rdd7.collect().toBuffer)




  }

}
