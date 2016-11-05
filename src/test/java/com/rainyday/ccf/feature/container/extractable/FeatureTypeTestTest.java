package com.rainyday.ccf.feature.container.extractable;

import static org.junit.Assert.*;

/**
 * Created by haifwu on 2016/11/5.
 */
public class FeatureTypeTestTest {

    public static void testOrdinal(){
        for(FeatureType type: FeatureType.values()){
            System.out.println(type.toString() + ": " + type.ordinal());
        }
    }

    public static void testValueOf(){
        FeatureType type = FeatureType.valueOf("USER_BEHAVIOUR");
        System.out.println(type.ordinal());
    }

    public static void main(String[] args){
        testBadValueOf();
    }

    public static void testBadValueOf(){
        try{
            FeatureType type = FeatureType.valueOf("user_name");
            System.out.println(type.ordinal());
        } catch (IllegalArgumentException ignore){
            System.out.println("Bad parameters");
        }
        System.out.println("Next Step!");
    }
}