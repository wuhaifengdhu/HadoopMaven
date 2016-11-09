/*
    This script used to caculate the user's history.
 */
 REGISTER $JAR;

 SET pig.exec.reducers.max 999;
 SET pig.exec.reducers.bytes.per.reducer 536870912;
 SET mapred.job.queue.name $HADOOP_QUEUE;
 SET job.name 'CCF Generate Final Result';
 SET mapred.child.java.opts -Xmx1G;
 SET mapred.child.ulimit 2.5G;
 SET mapred.reduce.slowstart.completed.maps 0.6;
 SET mapred.map.tasks.speculative.execution true;
 SET mapred.reduce.tasks.speculative.execution true;
 SET mapreduce.output.fileoutputformat.compress false;


raw = LOAD '$SOURCE' USING PigStorage(',', '-schema');
raw = FOREACH raw GENERATE (int)score::User_id,(int)score::Merchant_id,(int)score::Coupon_id, ((float)score::mean)
/100);

STORE raw INTO '$OUPUT' USING PigStorage(',', '-schema');
