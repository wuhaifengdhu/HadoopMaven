package com.rainyday.ccf.feature.udf;

import com.rainyday.ccf.feature.util.CcfConstants;
import com.rainyday.ccf.feature.util.CcfUtils;
import org.apache.log4j.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import java.io.IOException;

/**
 * @author haifwu
 */
public class SmallThan extends EvalFunc<String> {
    private static final Logger LOG = Logger.getLogger(SmallThan.class);

    @Override
    public String exec(Tuple input) throws IOException {
        if(input.size() < 2) return "0";
        if(null == input.get(0) || null == input.get(1)){
            return "0";
        }
        float first = CcfUtils.getFloatValue(String.valueOf(input.get(0)));
        float second = CcfUtils.getFloatValue(String.valueOf(input.get(1)));
        return  (first < second) ? "1" : "0";
    }
}
