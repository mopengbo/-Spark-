
package org.example

import scala.io.StdIn
import scala.io.Source
import com.huaban.analysis.jieba.JiebaSegmenter
import org.apache.spark.{SparkConf, SparkContext}

object sousuoyinqing {
  def main(args: Array[String]): Unit = {
    println("请输入要查询的内容:")
    val name: String = StdIn.readLine()
    print(sousuo(name))
  }

  def sousuo(str: String): String = {

    print(str)
    val result: java.util.List[String] = new JiebaSegmenter().sentenceProcess(str)
    println(result)

    for (word <- result.toArray()) {
      var Map_ID :Map[String,String] = null
      var list :List[String] = null

      val file = Source.fromFile("D:\\Download\\language\\IDEA\\CaiJingYinQing\\data\\TF-IDFjuzhen_easy\\part-00000")


      for (line <- file.getLines.toList) {
        val lin = line.split(",").toList
        if( lin(0).replaceAll(" |\\(|\\)|[List]", "")== word ){
          println(word+"已找到！")
          //println(lin)
          for(i<-1 to lin.length-1){
            if(i%2!=0){
              val id =  lin(i).replaceAll(" |\\(|\\)|[List]", "")
              println("文章ID: "+id)
              val file2 = Source.fromFile("D:\\Download\\language\\IDEA\\CaiJingYinQing\\data\\zhaiyao\\part-00000")
              for (line <- file2.getLines.toList){
                if(line.split(",").toList(0).replaceAll(" |\\(|\\)|[List]", "")==id){
                  println("正文摘要: "+line.split(",").toList(1).replaceAll(" |\\(|\\)|[List]", ""))

                }
              }
              file2.close
            }
          }
        }
      }
      println("\n\n\n\n\n\n")
      file.close

    }

    return "搜索完毕！"
  }

}

