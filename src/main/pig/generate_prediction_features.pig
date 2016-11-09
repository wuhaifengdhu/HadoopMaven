/*
    This script used to caculate the user's history.
 */
 REGISTER $JAR;

 SET pig.exec.reducers.max 999;
 SET pig.exec.reducers.bytes.per.reducer 536870912;
 SET mapred.job.queue.name $HADOOP_QUEUE;
 SET job.name 'CCF Generate Features';
 SET mapred.child.java.opts -Xmx1G;
 SET mapred.child.ulimit 2.5G;
 SET mapred.reduce.slowstart.completed.maps 0.6;
 SET mapred.map.tasks.speculative.execution true;
 SET mapred.reduce.tasks.speculative.execution true;
 SET mapreduce.output.fileoutputformat.compress false;

DEFINE couponUseWith15Days           com.rainyday.ccf.feature.udf.CouponUsedIn15Days();
DEFINE generateDiscountFeature       com.rainyday.ccf.feature.udf.DiscountFeature();
DEFINE getDistance                   com.rainyday.ccf.feature.udf.GetDistance();


raw = LOAD '$SOURCE' USING PigStorage(',') AS (User_id:chararray, Merchant_id:chararray, Coupon_id:chararray,
  Discount_rate_str:chararray, Distance_str:chararray, Date_received:chararray);

raw = FOREACH raw GENERATE User_id, Merchant_id, Coupon_id, generateDiscountFeature(Discount_rate_str) AS keys:
(discountRate: float, isFixedDiscount:chararray), getDistance(Distance_str) as distance;

raw = FOREACH raw GENERATE User_id, Merchant_id, Coupon_id, FLATTEN(keys), distance;

STORE raw INTO '$OUTPUT' USING PigStorage('|');