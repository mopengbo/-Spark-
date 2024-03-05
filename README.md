# -Spark-
一、实验目的
建立搜索引擎，输入查询内容后自动推送按相关度排序的文章的url哈希值及正文摘要。从财经新闻网页数据开始，进行正文提取、中文分词、倒排索引构建、执行搜索。

二、实验步骤
（1）使用java，spark等多种方式提取news，report，notice三个项目的正文提取，采用正则表达式提取指定网站栏目新闻的标题、正文和发表时间。

（2）中文分词，将正文进行中文分词，保存每个新闻的URL、标题、正文等数据，存储到本地及hdfs。

（3）倒排索引构建，将词汇、次数和文章ID构建成倒排索引和对应的TF值，构建TF-IDF矩阵，保存到本地及hdfs。

（4）执行搜索，对用户搜索词进行分词，从倒排索引读取对应词汇，读取TF值，读取数据计算IDF值，根据IF×IDF值对词汇对应的文章进行排序，显示排序后的正文摘要

（5）jar包在曙光大数据平台运行

三、实验结果
（一）正文提取
1.java提取news
![image](https://github.com/mopengbo/-Spark-/assets/72540039/42053ed2-4ed6-43ee-8ca6-f864e63c68ae)
![image](https://github.com/mopengbo/-Spark-/assets/72540039/0a173cbc-8e33-4f26-a1bc-4d90688e8235)

观察html结构，分别构造标题、日期、正文的re表达式

![image](https://github.com/mopengbo/-Spark-/assets/72540039/a014978f-a8db-4ee1-b501-bca1513eff0d)
![image](https://github.com/mopengbo/-Spark-/assets/72540039/d8f168d3-cf32-4c22-84f6-85beeb7d744b)

创建java文件读写对象

![image](https://github.com/mopengbo/-Spark-/assets/72540039/6e3c5cee-df60-42cf-8c85-82d6f3aa7132)

遍历每篇文章，提取对应内容

![image](https://github.com/mopengbo/-Spark-/assets/72540039/e0db372e-4728-4c92-8e77-61a0ef4c5a43)

提取正文时先确定正文区域，在利用re进一步筛选

![image](https://github.com/mopengbo/-Spark-/assets/72540039/274757c2-c378-4789-b7a9-4be3e10ecf86)

查看提取结果


2.Scala提取report、notice

![image](https://github.com/mopengbo/-Spark-/assets/72540039/dafad638-5485-477c-b5b7-903d74f32e85)
![image](https://github.com/mopengbo/-Spark-/assets/72540039/7aed1c0e-c431-4aa1-acd8-93d38c48a3cb)

提取正则表达式

![image](https://github.com/mopengbo/-Spark-/assets/72540039/4d2840bf-2367-4369-9b45-ecb8e8386d15)

读取文件


![image](https://github.com/mopengbo/-Spark-/assets/72540039/1481e265-1ea6-4194-b6fb-7e70845a2f8f)

遍历每篇文章提取相应内容


![image](https://github.com/mopengbo/-Spark-/assets/72540039/cbb327c7-216a-4771-95c7-7ccc11bea4c1)
![image](https://github.com/mopengbo/-Spark-/assets/72540039/e67c23f4-b259-4860-9526-68b7eb486b8c)

将提取内容保存到本地查看


3.spark rdd 提取正文

![image](https://github.com/mopengbo/-Spark-/assets/72540039/8e720457-2aba-4b2d-96be-86088960c55c)

读取文章至RDD，并定义正则表达式

![image](https://github.com/mopengbo/-Spark-/assets/72540039/069707a9-e813-4da2-85f4-c77386839c1b)

利用map方法对每篇文章进行遍历提取，并用sustring，droprighr字符串提取方法对正文进行进一步提取

![image](https://github.com/mopengbo/-Spark-/assets/72540039/c51cfe5a-8978-4ebe-9f11-797c685f3803)

成功利用spark提取news的url，标题及正文，其中url转化为哈希值形式保存



（二）中文分词


![image](https://github.com/mopengbo/-Spark-/assets/72540039/2d4ee2af-d187-40b6-a8f3-3ebbbf3686ee)

提取需要分词的文章内容

![image](https://github.com/mopengbo/-Spark-/assets/72540039/0ca67c56-34d7-4f09-8275-9852e2296c00)

提取后的rdd格式内容

![image](https://github.com/mopengbo/-Spark-/assets/72540039/ccc68b83-7e67-44f2-b32c-562e1acfd93c)

分词过程：首先利用map方法将url的哈希值作为key，对正文利用jieba分词成数组后将其转换成（单词，1）的形式随后根据单词分组并利用size计算出每个单词的次数，转成可显示的list列表，最后将分词结果保存至本地和hdfs


![image](https://github.com/mopengbo/-Spark-/assets/72540039/59f96e8e-792e-4dce-9c9c-2fd91708720f)

在本地查看分词结果

![image](https://github.com/mopengbo/-Spark-/assets/72540039/cca12904-1a3d-4cd9-a269-0627cfd43e49)

在hdfs中查看分词结果


（三）倒排索引构建

![image](https://github.com/mopengbo/-Spark-/assets/72540039/34dd804b-1628-46b8-9c13-4b7f16c0bad0)

编写代码构建词频矩阵并保存至文件

![image](https://github.com/mopengbo/-Spark-/assets/72540039/840d018a-94e3-49f3-82d4-a99e82ae83d5)

首先利用map将分词结果转化为RDD[Iterable[单词,(ID,次数)]]的形式

![image](https://github.com/mopengbo/-Spark-/assets/72540039/ec9aca92-f4ff-483e-a053-05fc3090f6b0)

随后，为了进行下一步操作，利用flatmap方法将rdd降维成RDD[单词,(ID,次数)]的形式

![image](https://github.com/mopengbo/-Spark-/assets/72540039/eaf1d2eb-ac79-473b-9ced-f353e24fae3e)

最后利用groupbykey方法将元素按单词分组并将value改为tolist形式便于显示


![image](https://github.com/mopengbo/-Spark-/assets/72540039/e22e1c8f-2661-435f-8d06-8d884710f599)

在本地查看倒排索引结果


![image](https://github.com/mopengbo/-Spark-/assets/72540039/8efd0a8f-1562-4761-b531-3ef284faecfe)

在hdfs查看倒排索引结果




（四）构建TF-IDF矩阵

![image](https://github.com/mopengbo/-Spark-/assets/72540039/eac03483-d925-48b5-9948-3f15736fc4fc)

首先提取正文并length计算文章字数并用count计算总网页数

![image](https://github.com/mopengbo/-Spark-/assets/72540039/74296da1-e694-4a3c-857c-9aad66c8a1a6)

利用zip及map方法将文章字数加入到分词结果rdd中

![image](https://github.com/mopengbo/-Spark-/assets/72540039/446d9994-bf1e-45b4-a837-ed5c184ca514)

在构造词频矩阵的基础上通过map及乘除法运算求出每个单词中每篇文章的TF、IDF及TF*IDF值（其中单词在每篇文章中的TF=出现次数/字数，IDF为总网页数/单词出现的网页数）

![image](https://github.com/mopengbo/-Spark-/assets/72540039/a8285cf9-5064-4157-8152-60206b655116)

将TF-IDF矩阵分别保存到本地及hdfs

![image](https://github.com/mopengbo/-Spark-/assets/72540039/2c14843f-8306-45a6-9b68-7d05ae3e10d0)

含义：RDD[(单词, List[(（ID,字数）, 次数, TF, IDF, TF*IDF)])]

![image](https://github.com/mopengbo/-Spark-/assets/72540039/fa2fdb1e-4c5c-41a5-8136-13d02827d2be)

在hdfs中查看TF-IDF矩阵



（五）执行搜索

![image](https://github.com/mopengbo/-Spark-/assets/72540039/5f71ded5-b4aa-434c-bdde-64442b3ae74b)

首先将每个单词列表中的ID根据TF*IDF相关性由大到小降序排序

![image](https://github.com/mopengbo/-Spark-/assets/72540039/6bcb1db4-3905-46b7-adf6-4148d3affca7)

 编写scala主函数，将输入的句子进行分词成列表

![image](https://github.com/mopengbo/-Spark-/assets/72540039/2b605c09-5e66-477c-aeac-4266d85b5833)

对每个单词都在TF-IDF矩阵中遍历索引，找到匹配单词后将该词的url哈希值及正文摘要打印推送给用户

示例：
![image](https://github.com/mopengbo/-Spark-/assets/72540039/dbf160a0-2da8-4b0f-884c-2c36221f44fd)
搜索句子“证券欺诈”

![image](https://github.com/mopengbo/-Spark-/assets/72540039/f306702b-899b-4e61-9384-14202253a3ad)
对句子进行分词



![image](https://github.com/mopengbo/-Spark-/assets/72540039/c7663cb0-34e3-4782-869d-2be99cb8600e)
![image](https://github.com/mopengbo/-Spark-/assets/72540039/0838fe08-d6ae-4fa1-b359-4134ce7032af)
查询结果



（六）jar包曙光平台运行

![image](https://github.com/mopengbo/-Spark-/assets/72540039/37e1cbe7-45ad-44e8-be3b-6aeaaaf6ce00)
修改代码目录部分，使其符合曙光平台规则

![image](https://github.com/mopengbo/-Spark-/assets/72540039/dbda3050-24f3-4728-884a-dbb55af96f06)
打包

![image](https://github.com/mopengbo/-Spark-/assets/72540039/744a7c82-0419-47db-bbb6-63c2fdb75a39)
编译

![image](https://github.com/mopengbo/-Spark-/assets/72540039/84a3e61c-e28c-4840-b436-f29dd1cc27fd)
在output中生成了jar包

![image](https://github.com/mopengbo/-Spark-/assets/72540039/97c49228-bc25-4423-8bcb-75ce3cd5c7a3)
执行spark-submit命令

![image](https://github.com/mopengbo/-Spark-/assets/72540039/09dc6c96-01fb-4a27-bb69-d9785dc913b7)
![image](https://github.com/mopengbo/-Spark-/assets/72540039/38e4d809-4458-4215-9063-43f17ac69825)
查看分词结果

![image](https://github.com/mopengbo/-Spark-/assets/72540039/de4a17eb-69d7-4a96-a7c6-05fd88119d87)
查看词频矩阵




