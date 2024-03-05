package org.example

import scala.io.Source
import java.io.PrintWriter
import java.io.File

object notice {
  def main(args: Array[String]): Unit = {

    //正则表达式规则
    val lineRegex_date = f"""((?<=>公告日期:)[^</td]*)""".r
    val lineRegex_title = """((?<=<title>)[^</]*)""".r
    val lineRegex_text = """(?<=<p>)[\s\S]*?(?=</p><br>)""".r
    //读取文件
    val sourse = Source.fromFile("D:\\Download\\language\\IDEA\\CaiJingYinQing\\sample_files\\sample_files\\sina_notice_sample_combined.txt")
    val writer = new PrintWriter(new File("D:\\Download\\language\\IDEA\\CaiJingYinQing\\data\\notice\\notice.txt"))
    val lineIterator = sourse.getLines()

    for (line <- lineIterator) {
      if ((lineRegex_title findAllIn line toList).toString() != "List()") {
        //println((lineRegex_title findAllIn line toList).toString())
        var str: String = (lineRegex_title findAllIn line toList).toString()
        var str2: String = str.substring(5).dropRight(1)
        println(str2)
        writer.write("标题:  ")
        writer.write(str2)
        writer.write("   ")
      }
      if ((lineRegex_date findAllIn line toList).toString() != "List()") {
        //println((lineRegex_date findAllIn line toList).toString())
        var str: String = (lineRegex_date findAllIn line toList).toString()
        var str2: String = str.substring(5).dropRight(1)
        println(str2)
        writer.write("日期:   ")
        writer.write(str2)
        writer.write("   ")
      }
      if ((lineRegex_text findAllIn line toList).toString() != "List()") {
        val str = (lineRegex_text findAllIn (line) toList).toString().substring(5).dropRight(1)
        //使用正则表达式替换方法去除无用字符
        val newString = str.replaceAll("<blockquote>.*?</blockquote>|<[^>]*>|[\r\n ]{2,}|", "")
        println(newString)
        writer.write("正文:   ")
        writer.write(newString)
        writer.write("\n")
      }
    }
    writer.close()
  }

}

