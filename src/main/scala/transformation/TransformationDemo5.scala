package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

/**
 * @ProjectName: SparkCore 
 * @Package:
 * @ClassName: TransformationDemo5
 * @Author: Administrator
 * @Description: ${description}
 * @Date:    2020/3/17 9:06
 * @Version:    1.0
*/
object TransformationDemo5 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("TransformationDemo5")
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6), 3)
    println(rdd.partitions.length)
    //repartition:对rdd进行重新分区
    //repartition默认会执行shuffle，即对分区中的数据重新计算
    val value: RDD[Int] = rdd.repartition(6)
    println(value)
    println(value.partitions.length)
    //coalesce算子，也可以对rdd进行重新分区，但只能小于原有分区的值。
    //coalesce不会进行shuffle
    val value1: RDD[Int] = rdd.coalesce(2)
    println(value1.partitions.length)
    //
    val rdd2: RDD[(String, Int)] = sc.parallelize(List(("e", 5), ("c", 3), ("d", 4), ("c", 2)), 2)
    val rdd2_1: RDD[(String, Int)] = rdd2.partitionBy(new HashPartitioner(4))
    println(rdd2.partitions.length)
    println(rdd2_1.partitions.length)

//    rdd2.repartitionandsortwithinpartitions是reparation算子的变种，在使用完reparation算子重新分区之后，还需要进行排序
    val rdd3: RDD[(Int, String)] = sc.parallelize(List((3, "cc"), (4, "bb"), (1, "aa")), 2)
    rdd3.repartitionAndSortWithinPartitions(new HashPartitioner(4)).foreach(println)

  }
}
