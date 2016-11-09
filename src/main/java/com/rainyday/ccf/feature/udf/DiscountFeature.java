package com.rainyday.ccf.feature.udf;

import com.rainyday.ccf.feature.container.data.AbstractData;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;

/**
 * @author haifwu
 */
public class DiscountFeature extends EvalFunc<Tuple> {

    @Override
    public Tuple exec(Tuple input) throws IOException {
        String discountStr = (String) input.get(0);
        Tuple tuple = TupleFactory.getInstance().newTuple();
        tuple.append(AbstractData.getDiscountRateFromString(discountStr));
        tuple.append(AbstractData.isFixedDiscountRateFromString(discountStr) ? "1": "0");
        return tuple;
    }
}
