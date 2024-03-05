package org.example;

import java.util.regex.*;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class report {
    public static void main (String args[ ]) throws IOException {

        File f = new File("D:\\Download\\language\\IDEA\\CaiJingYinQing\\sample_files\\sample_files\\sina_news_sample_combined.txt");
        File out = new File("out.txt");
        File out2 = new File("zhengwen.txt");


        Pattern pattern_title = Pattern.compile("(?<=title\" content=\")[^\"]*");
        Pattern pattern_date = Pattern.compile("(?<=<span class=\"date\">)[^<]*");
        Pattern pattern_from = Pattern.compile("(?<=rel=\"nofollow\">)[^<]*");
        Pattern pattern_comment = Pattern.compile("(?<=<!-- 行情图end -->)[\\s\\S]*?(?=<!--)");
        Pattern pattern_comment2 = Pattern.compile("<blockquote>.*?</blockquote>|<[^>]*>|[\\r\\n ]{2,}");
        //Pattern pattern_url = Pattern.compile("(?<=a target[^.]{1,20}?)https://finance.+?\\b{4}-[^.]+?\\b{7}.shtml");

        BufferedReader br = new BufferedReader(new FileReader(f));
        BufferedWriter bw = new BufferedWriter(new FileWriter(out));
        BufferedWriter bw2 = new BufferedWriter(new FileWriter(out2));

        String s = null;


        while ((s = br.readLine()) != null) {
            Matcher matcher_title = pattern_title.matcher(s);
            while (matcher_title.find()) { //提取标题
                bw.write("标题: ");
                bw.write(matcher_title.group());
                bw.write(" ");
            }

            Matcher matcher_date = pattern_date.matcher(s);
            while (matcher_date.find()) { //提取日期
                bw.write("日期: ");
                bw.write(matcher_date.group());
                bw.write(" ");
            }

            Matcher matcher_from = pattern_from.matcher(s);
            while (matcher_from.find()) { //提取来源
                bw.write("来源: ");
                bw.write(matcher_from.group());
                bw.write(" ");
            }

            Matcher matcher_comment = pattern_comment.matcher(s);
            while (matcher_comment.find()) { //提取正文
                Matcher m = pattern_comment2.matcher(matcher_comment.group());//这里把想要替换的字符串传进来
                String newString = m.replaceAll("").trim();//将替换后的字符串存在变量newString中
                bw.write("正文: ");
                bw.write(newString);
                String sd = String.valueOf(newString);
                bw2.write(sd);
                bw.write("\n");
            }

            bw.newLine();
            bw2.newLine();
            bw.flush();
            bw2.flush();

        }
    }
}
