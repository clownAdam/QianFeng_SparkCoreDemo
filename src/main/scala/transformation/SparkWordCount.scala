package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark版本的workcount
 */
object SparkWordCount {
  def main(args: Array[String]): Unit = {
    //配置spark
    val conf: SparkConf = new SparkConf().setAppName("SparkWordCount")
    //创建sparkcontext对象
    val sc = new SparkContext(conf)

    //读取文件
    val lines: RDD[String] = sc.textFile(args(0))
    //切分数据
    val words: RDD[String] = lines.flatMap(_.split(" "))
    //将单词组成元祖
    val tuples: RDD[(String, Int)] = words.map((_, 1))
    //求和
    val sumed: RDD[(String, Int)] = tuples.reduceByKey(_ + _)
    //排序 false：降序
    val sorted: RDD[(String, Int)] = sumed.sortBy(_._2, false)
    //将当前数据提交到集群中存储
    sorted.saveAsTextFile(args(1))
    sc.stop()

  }
}
