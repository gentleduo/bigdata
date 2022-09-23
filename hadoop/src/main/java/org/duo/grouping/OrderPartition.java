package org.duo.grouping;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderPartition extends Partitioner<OrderBean, Text> {

    /**
     * 分区规则: 根据订单的ID实现分区
     * 如果不自定义OrderPartition的话，默认会按照orderBean的hashCode来分区，那么相同的订单id就会被分在不同的分区
     *
     * @param orderBean K2
     * @param text      V2
     * @param i         ReduceTask个数
     * @return 返回分区的编号
     */
    @Override
    public int getPartition(OrderBean orderBean, Text text, int i) {
        return (orderBean.getOrderId().hashCode() & 2147483647) % i;
    }
}