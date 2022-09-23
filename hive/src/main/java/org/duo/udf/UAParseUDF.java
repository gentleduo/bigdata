package org.duo.udf;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.UDF;

public class UAParseUDF extends UDF {

    public String evaluate(final String userAgent){
        StringBuilder builder = new StringBuilder();
        UserAgent ua = new UserAgent(userAgent);
        builder.append(ua.getOperatingSystem()+"\t"+ua.getBrowser()+"\t"+ua.getBrowserVersion());
        return  (builder.toString());
    }
}