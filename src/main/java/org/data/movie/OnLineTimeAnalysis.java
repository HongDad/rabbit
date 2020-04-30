package org.data.movie;

import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.data.movie.bean.MovierReviewBean;
import org.data.utils.MySQLTextOutputFormat;

import java.io.IOException;

/**
 * 电影播放时长的分析，精确到分钟级
 */
public class OnLineTimeAnalysis {
    static class OnLineTimeMapper extends Mapper<LongWritable, Text, Text, MovierReviewBean> {
        /**
         * map方法一行数据执行一次  1000
         * 将new Gson(); 代码提到方法的外面 , 避免大量的对象的创建
         */
        Gson gs = new Gson();
        Text k = new Text();
        MovierReviewBean v = null;
        private LongWritable key;
        private Text value;
        private Context context;

        @Override
        protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, MovierReviewBean>.Context context)
                throws IOException, InterruptedException {
            this.key = key;
            this.value = value;
            this.context = context;
            try {
                String line = value.toString();
                //赋值
                v = gs.fromJson(line, MovierReviewBean.class);
                // k  赋值
                if (!v.getPlay_time().equals("")) {
                    k.set(v.getPlay_time().split(":")[0] +":"+ v.getPlay_time().split(":")[1] + "");
                }
                // 写出去
                context.write(k, v);
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    static class OnLineTimeReducer extends Reducer<Text, MovierReviewBean, Text, IntWritable> {
        IntWritable v = new IntWritable();

        @Override
        protected void reduce(Text uid, Iterable<MovierReviewBean> iters, Reducer<Text, MovierReviewBean, Text, IntWritable>.Context context)
                throws IOException, InterruptedException {
            int count = 0;  // 次数
            for (MovierReviewBean movie : iters) {
                count++;
            }
            int avgRate = count;
            v.set(avgRate);
            context.write(uid, v);
        }
    }

    public static void main(String[] args) throws Exception {
        // 获取初始化配置
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setMapperClass(OnLineTimeMapper.class);
        job.setReducerClass(OnLineTimeReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MovierReviewBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("D:\\file\\test\\movie.txt"));
        //FileOutputFormat.setOutputPath(job, new Path("D:\\file\\test\\output"));
        //outputformat输出到mysql中，自定义输出类
        job.setOutputFormatClass(MySQLTextOutputFormat.class);
        boolean b = job.waitForCompletion(true);

        System.exit(b ? 0 : -1);

    }
}
