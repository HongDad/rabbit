package com.data.console;

import com.data.map.DataMapper;
import com.data.reduce.DataReduce;
import com.data.utils.MySQLTextOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * Created by wdh .
 *
 * 这个类就是mr程序运行时候的主类，本类中组装了一些程序运行时候所需要的信息
 * 比如：使用的是那个Mapper类  那个Reducer类  输入数据在那 输出数据在什么地方
 */
public class DataDriver {
    public static void main(String[] args) throws Exception {

        //使用windows上的本地路径进行测试
        args = new String[]{".\\src\\main\\resources\\input"};
        Configuration conf = new Configuration();
        //获取job对象
        Job job = Job.getInstance(conf);

        //设置jar包
        job.setJarByClass(DataDriver.class);
        //关联mapper和reducer
        job.setMapperClass(DataMapper.class);
        job.setReducerClass(DataReduce.class);

        //设置map输出数据类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置最终输出数据类型kv
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //outputformat输出到mysql中，自定义输出类
        job.setOutputFormatClass(MySQLTextOutputFormat.class);

        //添加mysql驱动到类路径下，提交到集群时需要使用
        //job.addArchiveToClassPath(new Path("/lib/mysql/mysql-connector-java-5.1.27-bin.jar"));
        //设置输入输出文件路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //因为要输出到mysql中所以不用配置输出路径
        //FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交到yarn集群
        job.waitForCompletion(true);
    }
}
