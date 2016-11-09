package com.rainyday.ccf.feature.udf;

import com.rainyday.ccf.feature.util.CcfUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

/**
 * @author haifwu
 */
public class GetDistance extends EvalFunc<Integer> {

    @Override
    public Integer exec(Tuple input) throws IOException {
        String distanceStr = (String) input.get(0);
        return CcfUtils.getIntValue(distanceStr);
    }
}
