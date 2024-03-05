package org.example


import com.huaban.analysis.jieba.JiebaSegmenter
import com.huaban.analysis.jieba.JiebaSegmenter.SegMode
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import com.huaban.analysis.jieba.{JiebaSegmenter, SegToken}
import scala.util.matching.Regex




object Jieba {
  def main(args: Array[String]):Unit = {


    val sparkConf = new SparkConf()
    sparkConf.setMaster("local") //本地单线程运行
    sparkConf.setAppName("jiebafenci")
    val sc = new SparkContext(sparkConf)


    val rdd = sc.textFile("hdfs://HA/user/ud33/a/news.txt")
    //对每个句子执行分词，并转为词汇列表
    val zhengwen = new Regex("(?<=<!-- 行情图end -->)[\\s\\S]*?(?=<!--)")//正文
    val url = new Regex("\\t.*?(?=NWS)")//url
    val title = new Regex(f"""((?<=title" content=")[^"]*)""")//标题

    var rdd2 = rdd.map(x=>( ( (url findFirstIn x toList).toString().hashCode ,(title findFirstIn x toList).toString()) ,
      (zhengwen findFirstIn x toList).toString().replaceAll("<blockquote>.*?</blockquote>|<[^>]*>|[\r\n ]{2,}", "")
        .substring(5)
        .dropRight(1)))
      .map(x => (x._1._1.toString,  new JiebaSegmenter().sentenceProcess(x._2).toArray().map(x => (x.toString, 1)).toList.groupBy(x=>x._1).map(x=>(x._1,x._2.size)).toList))
      .repartition(1)
    rdd2.saveAsTextFile("hdfs://HA/user/ud33/b/fencijieguo") //// RDD[(ID, List[(word, 次数)])]

    val rdd3 = rdd2.map(x=>(x._2.map(y=>(y._1,(x._1,y._2))))).flatMap(x=>x).groupByKey().map(x=>(x._1,x._2.toList)) // RDD[(word, List[(ID, 次数)])]
    rdd3.saveAsTextFile("hdfs://HA/user/ud33/b/cipinjuzhen")

    val rdd4 = rdd.map(x=>( (url findFirstIn x toList).toString().hashCode.toString  ,
      (zhengwen findFirstIn x toList).toString().replaceAll("<blockquote>.*?</blockquote>|<[^>]*>|[\r\n ]{2,}", "")
        .substring(5)
        .dropRight(1).replaceAll("[\\p{Space}\\p{Zs}]+","").length)).repartition(1) //每个网页字数
    val sum = rdd4.count() //总网页数

    val rdd5 = rdd2.zip(rdd4).map(x=>((x._1._1,x._2._2),x._1._2)) // RDD[((ID, 网页字数), List[(word,次数)])]


    val rdd6 = rdd5.map(x=>(x._2.map(y=>(y._1,(x._1,y._2))))).flatMap(x=>x).groupByKey().map(x=>(x._1,x._2.toList))
      .map(x=>(x._1,(x._2.map(y=>(y._1,y._2,y._2.toFloat/y._1._2.toFloat,sum.toFloat/x._2.size.toFloat,
        (y._2.toFloat/y._1._2.toFloat)*(sum.toFloat/x._2.size.toFloat))))))                          //RDD[(word, List[(（ID,字数）, 次数, TF, IDF, TF*IDF)])]
    rdd6.saveAsTextFile("hdfs://HA/user/ud33/b/TF_IDF")
   // rdd6.saveAsTextFile("file:///D:\\Download\\language\\IDEA\\CaiJingYinQing\\data\\TF-IDFjuzhen")

    val rdd7 = rdd6.map(x=>(x._1,x._2.map(y=>(y._1._1,y._5)).sortBy(x=>x._2).reverse))
    //rdd7.saveAsTextFile("file:///D:\\Download\\language\\IDEA\\CaiJingYinQing\\data\\TF-IDFjuzhen_easy")

    //val rdd_title = rdd.map(x=>( (url findFirstIn x toList).toString().hashCode.toString ,(title findFirstIn x toList).toString()))
    //rdd_title.saveAsTextFile("file:///D:\\Download\\language\\IDEA\\CaiJingYinQing\\data\\zhaiyao")
    sc.stop()

  }
}
//(-2018643393,)
//(1644130518,财联社（北京，记者陈晨）讯     TF=次数/字数
