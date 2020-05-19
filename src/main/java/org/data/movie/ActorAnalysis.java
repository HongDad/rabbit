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
 * 演员数据分析
 */
public class ActorAnalysis {
    //静态类ActorMapper继承Mapper类
    static class ActorMapper extends Mapper<LongWritable, Text, Text, MovierReviewBean> {
    /**
     * map方法一行数据执行一次  1000
     * 将new Gson(); 代码提到方法的外面 , 避免大量的对象的创建
     */
    //new 一个Gson对象
    Gson gs = new Gson();
    //new 一个Text对象
    Text k = new Text();
    //定义一个初始值为空的电影类v
    MovierReviewBean v = null;
    //定义一个私有长整型变量
    private LongWritable key;
    //定义一个私有文本类型变量
    private Text value;
    //定义一个私有Context
    private Context context;
//重写map方法
    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //拿到传入的参数key,value,context
        this.key = key;
        this.value = value;
        this.context = context;
        //将处理逻辑进行异常处理
        try {
            //将拿到的value转为String类型
            String line = value.toString();
            //赋值
            //将拿到的vaue值转为Gson实体类
            v = gs.fromJson(line, MovierReviewBean.class);
            // k  赋值
            //拿到Gson实体类中的演员，并且按照，切开为一个演员数组
            String stars[] = v.getStars().split(",");
            //遍历拿到的演员
            for (int i = 0 ;i < stars.length;i++) {
                //将遍历的演员拼接上数据库表的名字赋值给key
                k.set(stars[i] + "-actor");
                // 写出去
                //将key写出
                context.write(k, v);
            }
            //抛异常
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }
}
//静态类ActorReducer继承Reducer
static class ActorReducer extends Reducer<Text, MovierReviewBean, Text, IntWritable> {
    //new 一个短整型类型的v
     IntWritable v = new IntWritable();
   //重写reduce方法
    @Override
    protected void reduce(Text uid, Iterable<MovierReviewBean> iters, Context context)
            throws IOException, InterruptedException {
        //定义一个int类型的count变量
        int count = 0;  // 次数
        //遍历拿到的迭代器iters
        for (MovierReviewBean movie : iters) {
            //进行count**
            count++;
        }
       //将统计后的count赋值给v
        v.set(count);
        //写出
        context.write(uid, v);
    }
}
//main方法
    public static void main(String[] args) throws Exception {
        // 获取初始化配置
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);
        //设置map类
        job.setMapperClass(ActorMapper.class);
        //设置reduce类
        job.setReducerClass(ActorReducer.class);
        //设置map类返回值的key的数据类型
        job.setMapOutputKeyClass(Text.class);
        //设置map类返回值的value的数据类型
        job.setMapOutputValueClass(MovierReviewBean.class);
        //设置reduce类返回值的key的数据类型
        job.setOutputKeyClass(Text.class);
        //设置reduce类返回值的value的数据类型
        job.setOutputValueClass(IntWritable.class);
         //设置文件的输入路径
        FileInputFormat.setInputPaths(job, new Path("D:\\file\\test\\movie.txt"));
        //FileOutputFormat.setOutputPath(job, new Path("D:\\file\\test\\output"));
        //outputformat输出到mysql中，自定义输出类
        job.setOutputFormatClass(MySQLTextOutputFormat.class);
        //提交job任务
        boolean b = job.waitForCompletion(true);
        //输出任务是否提交成功
        System.exit(b ? 0 : -1);

    }
}
