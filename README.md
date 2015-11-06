# HadoopMaven
Hadoop Maven Startup Project

# Project Description
This is a hadoop maven start up project!

#wordcount
1, generate bigdata-1.0-SNAPSHOT.jar.
>mvn clean package

2, copy to horton.

3, generate text file for test.
>vi wordcount.txt

4, copy wordcount.txt to hdfs.
>hadoop fs -copyFromLocal wordcount.txt

5, run hadoop job
>hadoop jar bigdata-1.0-SNAPSHOT.jar wordcount.txt output

6, cat the result
> hadoop fs -cat output/part-r-00000