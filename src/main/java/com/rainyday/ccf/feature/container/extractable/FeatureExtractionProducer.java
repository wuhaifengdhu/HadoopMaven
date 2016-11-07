package com.rainyday.ccf.feature.container.extractable;

import com.rainyday.ccf.feature.container.data.AbstractData;
import com.rainyday.ccf.feature.util.CcfUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Traversal FeatureType Enum, and generate <key, value> for mapper use;
 * @author haifwu
 */
public class FeatureExtractionProducer {
    private static final Logger LOG = LoggerFactory.getLogger(FeatureExtractionProducer.class);

    private Iterator<FeatureType> iterator;
    private AbstractData abstractData;
    private FeatureType currentType;

    public FeatureExtractionProducer(AbstractData data){
        this.iterator = Arrays.asList(FeatureType.values()).iterator();
        this.abstractData = data;
    }

    public boolean hasNext(){
        return iterator.hasNext();
    }

    public AbstractMap.SimpleEntry<String, String> next(){
        currentType = iterator.next();
        Extractable extractable = getCurrentExtractable();
        /**
         * If current feature type is not support, then return null
         */
        return null == extractable? null: new AbstractMap.SimpleEntry<String, String>(extractable.getKey(), extractable
                .getValue());
    }

    public Extractable getCurrentExtractable(){
        if(FeatureType.USER_BEHAVIOUR.equals(currentType)){
            return new UserBehaviourExtractable(abstractData);
        } else if(FeatureType.MERCHANT_BEHAVIOUR.equals(currentType)){
            return new MerchantBehaviourExtractable(abstractData);
        } else if(FeatureType.COUPON_TENDENCY.equals(currentType)){
            return new CouponTendencyExtractable(abstractData);
        } else {
            //TODO more extractable need added, if more enum being added.
            LOG.error("Invalid feature type has no extractable! current type = "  + CcfUtils.getNoNullString(currentType));
            return null;
        }
    }
    /*

    public static Computable getComputableByFeatureType(FeatureType type) {
        if(FeatureType.USER_BEHAVIOUR.equals(type)){
            return new UserBehaviourExtractable();
        } else if(FeatureType.MERCHANT_BEHAVIOUR.equals(type)){
            return new MerchantBehaviourExtractable();
        } else if(FeatureType.COUPON_TENDENCY.equals(type)){
            return new CouponTendencyExtractable();
        } else {
            //TODO more extractable need added, if more enum being added.
            LOG.error("Invalid feature type has no extractable! current type = "  + CcfUtils.getNoNullString(type));
            return null;
        }
    }*/
}
