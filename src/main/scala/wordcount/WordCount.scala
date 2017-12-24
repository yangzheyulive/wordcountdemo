package wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setAppName("filterCount")
    sparkConf.setMaster("local")

    val sc = new SparkContext(sparkConf)
    val rdd1 = sc.textFile("log")
    /**
      * sample算子：是一个transformation类的算子
      * 第一个参数：抽样的方式：true允许同一个数据抽取多次
      * 第二个参数：抽样的比例
      * 第三个参数：抽样算法的初始值
      */
    val sampleTrue = rdd1.sample(true, 0.5, 1)
    val sampleFalse = rdd1.sample(false,0.5,1)
    filterCount("sampleTrue", sampleTrue, rdd1)
    filterCount("sampleFalse", sampleFalse, rdd1)
    filterCount("full", rdd1, rdd1)
    sc.stop()

  }

  def filterCount(objName:String, rddTemp:RDD[String], sourceRdd:RDD[String]): Unit ={
    val rdd2 = rddTemp.map(x=> (x.split("\t")(1), 1))
    val rdd3 = rdd2.reduceByKey((v1:Int, v2:Int) => {v1 + v2})
    val rdd4 = rdd3.map(x=> x.swap)
    val rdd5 = rdd4.sortByKey(false)
    val array = rdd5.take(1)
    val firsName = array(0)._2
    val rdd6 = sourceRdd.filter(x=> !x.split("\t")(1).equals(firsName))
    //rdd6.saveAsTextFile("hdfs://hadoop-node1:9000/output/result")
    rdd6.saveAsTextFile("result")
  }
}
