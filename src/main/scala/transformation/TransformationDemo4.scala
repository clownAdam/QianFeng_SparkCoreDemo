package transformation

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @ProjectName: SparkCore 
 * @Package:
 * @ClassName: TransformationDemo4
 * @Author: Administrator
 * @Description: ${description}
 * @Date:    2020/3/17 7:01
 * @Version:    1.0
*/
object TransformationDemo4 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local").setAppName("TransformationDemo4")
    val sc = new SparkContext(conf)
    //mapPartitions  遍历出rdd集合中每一个元素,并对元素可以进行进一步操作
    val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6),3)
    //mappartitions是对每个分区中的数据进行迭代
    val rdd2: RDD[Int] = rdd1.mapPartitions(_.map(_ * 10))
//    rdd2.saveAsTextFile("output01")
    println(rdd2.collect().toList)
    //mapPartitionswithIndex 是对每个rdd中的分区的遍历操作
    val Iter = (index:Int,iter:Iterator[Int])=>{

      iter.map(x => "[partID:"+index+",value"+x+"]")
    }
    val value: RDD[String] = rdd1.mapPartitionsWithIndex(Iter)
    println(value.collect().toList)

    //sortBY 排序算子 参数1：对RDD中数据的一种处理方式；参数2：决定排序的顺序true--升序
    val rdd4: RDD[Int] = sc.parallelize(List(5, 3, 2, 1, 4, 6, 9, 7, 8))
    val rdd4_sort: RDD[Int] = rdd4.sortBy(x => x, false).filter(x => x!=5)
    println(rdd4_sort.collect().toList)

    //sortByKey 和sortBy类似，但是没有sortBy灵活
    //    sortByKey是根据key进行排序的。并且key实现order接口
    val test: RDD[(Int, String)] = sc.parallelize(Array((3, "aa"), (6, "bb"), (1, "cc")))
    val value1: RDD[(Int, String)] = test.sortByKey()
    println(value1.collect().toList)

    //

  }
}
