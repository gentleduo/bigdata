package org.duo.udf;

import org.apache.hadoop.hive.ql.exec.UDF;

public class GetLength extends UDF {

    public int evaluate(String var) {

        try {
            return var.length();
        } catch (Exception e) {
            return -1;
        }
    }
}
