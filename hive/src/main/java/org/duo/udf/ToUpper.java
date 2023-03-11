package org.duo.udf;

import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.Locale;

public class ToUpper extends UDF {

    public String evaluate(String var) {
        return var.toUpperCase();
    }
}
