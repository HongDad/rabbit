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
import java.util.Arrays;

/**
 * 不同评分的曝光数量分析
 */
public class TypeShowAnalysis {

    static class TypeShowMapper extends Mapper<LongWritable, Text, Text, MovierReviewBean> {
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
        protected void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            this.key = key;
            //定义一个时间集合
            String year[] = new String[]{"剧情","喜剧","动作","爱情","惊悚","犯罪","悬疑","战争","科幻","动画","恐怖","家庭","传记","冒险","奇幻","武侠","历史","运动","歌舞","音乐","记录","伦理","西部"};
            this.value = value;
            this.context = context;
            try {
                String line = value.toString();
                //赋值
                v = gs.fromJson(line, MovierReviewBean.class);
                // k  赋值
                String tages[] = v.getTags().split(",");
                for (int i = 0 ;i < tages.length;i++) {
                    if (Arrays.asList(year).contains(tages[i])) {
                        k.set(tages[i]+"-type_show");
                        // 写出去
                        context.write(k, v);
                    }
                }
            } catch (Exception e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    static class TypeShowReducer extends Reducer<Text, MovierReviewBean, Text, IntWritable> {
        IntWritable v = new IntWritable();

        @Override
        protected void reduce(Text uid, Iterable<MovierReviewBean> iters, Context context)
                throws IOException, InterruptedException {
            int count = 0;  // 次数
            Long sum=0L;
            for (MovierReviewBean movie : iters) {
                count++;
              String  hots = movie.getHot();
              String  hot1 = hots.substring(hots.length()-1);
              String  hot2 = hots.substring(0,hots.length()-1);
              if (Long.parseLong(hot2)<0){
                  System.out.println(hot2);              }
                if (hot1.equals("万")){
                    sum += Long.parseLong(hot2)*10000;
                }else  if (hot1.equals("亿")){
                    sum += Long.parseLong(hot2)*100000000;
                }else{
                    sum += Long.parseLong(hots);
                }
                System.out.println(sum);
            }
            Long avgRate =(sum/count);
            v.set(avgRate.intValue());
            context.write(uid, v);
        }
    }

    public static void main(String[] args) throws Exception {
        // 获取初始化配置
        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setMapperClass(TypeShowMapper.class);
        job.setReducerClass(TypeShowReducer.class);

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
