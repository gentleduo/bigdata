package org.duo.hbase.tableOperate;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class HBaseMain extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {

        Job job = Job.getInstance(super.getConf(), "hbaseMR");
        //打包运行，必须设置main方法所在的主类
        job.setJarByClass(HBaseMain.class);

        Scan scan = new Scan();
        //定义我们的mapper类和reducer类
        /**
         * String table, Scan scan,
         Class<? extends TableMapper> mapper,
         Class<?> outputKeyClass,
         Class<?> outputValueClass, Job job,
         boolean addDependencyJars
         */
        TableMapReduceUtil.initTableMapperJob("myuser", scan, HBaseSourceMapper.class, Text.class, Put.class, job, false);
        //使用工具类初始化reducer类
        TableMapReduceUtil.initTableReducerJob("myuser2", HBaseSinkReducer.class, job);
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }

    //程序入口类
    public static void main(String[] args) throws Exception {
        //Configuration conf, Tool tool, String[] args
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("hbase.zookeeper.quorum", "server01:2181,server02:2181,server03:2181");
        int run = ToolRunner.run(configuration, new HBaseMain(), args);
        System.exit(run);
    }
}
