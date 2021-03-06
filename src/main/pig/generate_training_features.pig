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
DEFINE smallThan                     com.rainyday.ccf.feature.udf.SmallThan();

/**
* Basic manipulate of data
*/
raw = LOAD '$SOURCE' USING PigStorage(',') AS (User_id:chararray, Merchant_id:chararray, Coupon_id:chararray,
  Discount_rate_str:chararray, Distance_str:chararray, Date_received:chararray, Date:chararray);

raw = FOREACH raw GENERATE User_id, Merchant_id, Coupon_id, generateDiscountFeature(Discount_rate_str) AS keys:
(discountRate: float, isFixedDiscount:chararray), getDistance(Distance_str) as distance, Date_received,
couponUseWith15Days(Date_received, Date) AS tag;

raw = FOREACH raw GENERATE User_id, Merchant_id, Coupon_id, Date_received, FLATTEN(keys), distance, tag;

/**
*  Add user behavior related features
*/
user = LOAD '$USER_DATA' USING PigStorage('|') AS (userId:chararray, offlineDirectBuyWithoutCouponTimes:int,
    offlineDirectBuyHaveCouponTimes:int, offlineCouponBuyWithin15DaysTimes:int, offlineCouponBuyOut15DaysTimes:int,
    offlineCouponBuyAverageDistance:float, offlineCouponBuyWithin15DaysAverageDiscount:float,
    offlineCouponBuyWithin15DaysIsFixedDiscountTimes:int, onlineClickCouponTimes:int, onlineCollectCouponTimes:int,
    onlineDirectBuyWithoutCouponTimes:int, onlineDirectBuyHaveCouponTimes:int, onlineCouponBuyWithin15DaysTimes:int,
    onlineCouponBuyOutOf15DaysTimes:int);

rawUser = JOIN raw by User_id left, user by userId;
rawUser = FOREACH rawUser GENERATE tag, User_id, Merchant_id, Coupon_id, discountRate, isFixedDiscount, distance,
Date_received, offlineDirectBuyWithoutCouponTimes,offlineDirectBuyHaveCouponTimes,offlineCouponBuyWithin15DaysTimes,
            offlineCouponBuyOut15DaysTimes,offlineCouponBuyAverageDistance,offlineCouponBuyWithin15DaysAverageDiscount,
            offlineCouponBuyWithin15DaysIsFixedDiscountTimes,onlineClickCouponTimes,onlineCollectCouponTimes,
            onlineDirectBuyWithoutCouponTimes,onlineDirectBuyHaveCouponTimes,onlineCouponBuyWithin15DaysTimes,
            onlineCouponBuyOutOf15DaysTimes;

/**
*  Add merchant behavior related features
*/
merchant = LOAD '$MERCHANT_DATA' USING PigStorage('|') AS (merchantId:chararray, recordTimes:int, clickTimes:int,
            collectionTimes:int, usedWithin15DaysTimes:int, usedOutOf15DaysTimes:int, averageDiscountRate:float);
rawUserMerchant = JOIN rawUser by Merchant_id left, merchant by merchantId;
rawUserMerchant = FOREACH rawUserMerchant GENERATE tag, User_id, Merchant_id, Coupon_id, discountRate,Date_received,
        isFixedDiscount, distance, offlineDirectBuyWithoutCouponTimes,offlineDirectBuyHaveCouponTimes,
        offlineCouponBuyWithin15DaysTimes, offlineCouponBuyOut15DaysTimes,offlineCouponBuyAverageDistance,
        offlineCouponBuyWithin15DaysAverageDiscount, offlineCouponBuyWithin15DaysIsFixedDiscountTimes,
        onlineClickCouponTimes,onlineCollectCouponTimes, onlineDirectBuyWithoutCouponTimes,onlineDirectBuyHaveCouponTimes,
        onlineCouponBuyWithin15DaysTimes,onlineCouponBuyOutOf15DaysTimes, recordTimes, clickTimes, collectionTimes,
        usedWithin15DaysTimes, usedOutOf15DaysTimes, averageDiscountRate;

/**
*  Add coupon tendency related feature
*/
coupon = LOAD '$COUPON_DATA' USING PigStorage('|') AS (couponId:chararray, coupon_recordTimes:int,
        coupon_clickTimes:int, coupon_collectionTimes:int, coupon_usedWithin15DaysTimes:int,
        coupon_usedOutOf15DaysTimes:int, coupon_averageDiscountRate:float );
rawUserMerchantCoupon = JOIN rawUserMerchant by Coupon_id left, coupon by couponId;
rawUserMerchantCoupon = FOREACH rawUserMerchantCoupon GENERATE tag, User_id, Merchant_id, Coupon_id, discountRate,
        isFixedDiscount, distance, offlineDirectBuyWithoutCouponTimes,offlineDirectBuyHaveCouponTimes,Date_received,
        offlineCouponBuyWithin15DaysTimes, offlineCouponBuyOut15DaysTimes,offlineCouponBuyAverageDistance,
        offlineCouponBuyWithin15DaysAverageDiscount, offlineCouponBuyWithin15DaysIsFixedDiscountTimes,
        onlineClickCouponTimes,onlineCollectCouponTimes, onlineDirectBuyWithoutCouponTimes,onlineDirectBuyHaveCouponTimes,
        onlineCouponBuyWithin15DaysTimes,onlineCouponBuyOutOf15DaysTimes, recordTimes, clickTimes, collectionTimes,
        usedWithin15DaysTimes, usedOutOf15DaysTimes, averageDiscountRate, coupon_recordTimes, coupon_clickTimes,
        coupon_collectionTimes, coupon_usedWithin15DaysTimes, coupon_usedOutOf15DaysTimes, coupon_averageDiscountRate;

/**
*  Add user merchant  related feature
*/
userMerchant = LOAD '${USER_MERCHANT_DATA}' USING PigStorage('|') AS (userId:chararray, merchantId:chararray,
umTimes:int);
rawUserMerchantCouponUM = JOIN rawUserMerchantCoupon by (User_id, Merchant_id) left, userMerchant by (userId,
merchantId);
rawUserMerchantCouponUM = FOREACH rawUserMerchantCouponUM GENERATE tag, User_id, Merchant_id, Coupon_id, discountRate,
          isFixedDiscount, distance, Date_received, offlineDirectBuyWithoutCouponTimes,offlineDirectBuyHaveCouponTimes,
          offlineCouponBuyWithin15DaysTimes, offlineCouponBuyOut15DaysTimes,offlineCouponBuyAverageDistance,
          offlineCouponBuyWithin15DaysAverageDiscount, offlineCouponBuyWithin15DaysIsFixedDiscountTimes,
          onlineClickCouponTimes,onlineCollectCouponTimes, onlineDirectBuyWithoutCouponTimes,onlineDirectBuyHaveCouponTimes,
          onlineCouponBuyWithin15DaysTimes,onlineCouponBuyOutOf15DaysTimes, recordTimes, clickTimes, collectionTimes,
          usedWithin15DaysTimes, usedOutOf15DaysTimes, averageDiscountRate, coupon_recordTimes, coupon_clickTimes,
          coupon_collectionTimes, coupon_usedWithin15DaysTimes, coupon_usedOutOf15DaysTimes,
          coupon_averageDiscountRate, umTimes;

/**
*  Add user coupon  related feature
*/
userCoupon = LOAD '${USER_COUPON_DATA}' USING PigStorage('|') AS (userId:chararray, couponId:chararray, ucTimes:int);
rawUserMerchantCouponUM_UC = JOIN rawUserMerchantCouponUM by (User_id, Coupon_id) left, userCoupon by (userId,couponId);
rawUserMerchantCouponUM_UC = FOREACH rawUserMerchantCouponUM_UC GENERATE tag, User_id, Merchant_id, Coupon_id,
discountRate, isFixedDiscount, distance, Date_received, offlineDirectBuyWithoutCouponTimes,offlineDirectBuyHaveCouponTimes,
          offlineCouponBuyWithin15DaysTimes, offlineCouponBuyOut15DaysTimes,offlineCouponBuyAverageDistance,
          offlineCouponBuyWithin15DaysAverageDiscount, offlineCouponBuyWithin15DaysIsFixedDiscountTimes,
          onlineClickCouponTimes,onlineCollectCouponTimes, onlineDirectBuyWithoutCouponTimes,onlineDirectBuyHaveCouponTimes,
          onlineCouponBuyWithin15DaysTimes,onlineCouponBuyOutOf15DaysTimes, recordTimes, clickTimes, collectionTimes,
          usedWithin15DaysTimes, usedOutOf15DaysTimes, averageDiscountRate, coupon_recordTimes, coupon_clickTimes,
          coupon_collectionTimes, coupon_usedWithin15DaysTimes, coupon_usedOutOf15DaysTimes,
          coupon_averageDiscountRate, umTimes, ucTimes;

--STORE rawUserMerchantCouponUM_UC INTO 'wuhaifeng_checkpoint' USING PigStorage(',', '-schema');
/**
*  Add user discount  related feature
*/
userDiscount = LOAD '${USER_DISCOUNT_DATA}' USING PigStorage('|') AS (userId:chararray, udcDiscount:float);
rawUserMerchantCouponUM_UC_UDC = JOIN rawUserMerchantCouponUM_UC by User_id left, userDiscount by userId;

rawUserMerchantCouponUM_UC_UDC = FOREACH rawUserMerchantCouponUM_UC_UDC GENERATE tag, User_id, Merchant_id, Coupon_id,
discountRate, isFixedDiscount, distance, Date_received, offlineDirectBuyWithoutCouponTimes,offlineDirectBuyHaveCouponTimes,
          offlineCouponBuyWithin15DaysTimes, offlineCouponBuyOut15DaysTimes,offlineCouponBuyAverageDistance,
          offlineCouponBuyWithin15DaysAverageDiscount, offlineCouponBuyWithin15DaysIsFixedDiscountTimes,
          onlineClickCouponTimes,onlineCollectCouponTimes, onlineDirectBuyWithoutCouponTimes,onlineDirectBuyHaveCouponTimes,
          onlineCouponBuyWithin15DaysTimes,onlineCouponBuyOutOf15DaysTimes, recordTimes, clickTimes, collectionTimes,
          usedWithin15DaysTimes, usedOutOf15DaysTimes, averageDiscountRate, coupon_recordTimes, coupon_clickTimes,
          coupon_collectionTimes, coupon_usedWithin15DaysTimes, coupon_usedOutOf15DaysTimes,
          coupon_averageDiscountRate, umTimes, ucTimes, smallThan(udcDiscount, discountRate) AS greatUDC;

/**
*  Add user distance  related feature
*/
userDistance = LOAD '${USER_DISTANCE_DATA}' USING PigStorage('|') AS (userId:chararray, udiDistance:float);
rawUserMerchantCouponUM_UC_UDC_UDI = JOIN rawUserMerchantCouponUM_UC_UDC by User_id left, userDistance by userId;

rawUserMerchantCouponUM_UC_UDC_UDI = FOREACH rawUserMerchantCouponUM_UC_UDC_UDI GENERATE tag, User_id, Merchant_id, Coupon_id,
discountRate, isFixedDiscount, distance, Date_received, offlineDirectBuyWithoutCouponTimes,offlineDirectBuyHaveCouponTimes,
          offlineCouponBuyWithin15DaysTimes, offlineCouponBuyOut15DaysTimes,offlineCouponBuyAverageDistance,
          offlineCouponBuyWithin15DaysAverageDiscount, offlineCouponBuyWithin15DaysIsFixedDiscountTimes,
          onlineClickCouponTimes,onlineCollectCouponTimes, onlineDirectBuyWithoutCouponTimes,onlineDirectBuyHaveCouponTimes,
          onlineCouponBuyWithin15DaysTimes,onlineCouponBuyOutOf15DaysTimes, recordTimes, clickTimes, collectionTimes,
          usedWithin15DaysTimes, usedOutOf15DaysTimes, averageDiscountRate, coupon_recordTimes, coupon_clickTimes,
          coupon_collectionTimes, coupon_usedWithin15DaysTimes, coupon_usedOutOf15DaysTimes,
          coupon_averageDiscountRate, umTimes, ucTimes, greatUDC, smallThan(distance, udiDistance) AS nearUDI;

STORE rawUserMerchantCouponUM_UC_UDC_UDI INTO '$OUTPUT' USING PigStorage('|');



