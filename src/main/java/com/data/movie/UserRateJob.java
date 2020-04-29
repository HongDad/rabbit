package com.data.movie;

import com.data.movie.bean.Movie;
import com.data.utils.MySQLTextOutputFormat;
import com.google.gson.Gson;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;


public class UserRateJob {

	static class UserAvgRateMapper extends Mapper<LongWritable, Text, Text, Movie> {
		/**
		 * map方法一行数据执行一次  1000 
		 * 将new Gson(); 代码提到方法的外面 , 避免大量的对象的创建
		 */
		Gson gs = new Gson();
		Text k = new Text() ;
		Movie v  = null ;
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Movie>.Context context)
				throws IOException, InterruptedException {
			try {
				String line = value.toString();
				//赋值
				v = gs.fromJson(line, Movie.class);
				// k  赋值
				k.set(v.getUid()+"");
				// 写出去
				context.write(k, v);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	static class UserAvgRateReducer extends Reducer<Text, Movie, Text, IntWritable> {
		IntWritable v = new IntWritable() ;
		@Override
		protected void reduce(Text uid, Iterable<Movie> iters, Reducer<Text, Movie, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int count = 0 ;  // 次数
			int sum = 0 ;
			for (Movie movie : iters) {
				count++ ;
				sum+=movie.getRate() ;
			}
			int avgRate = sum  ;
			v.set(avgRate);
			context.write(uid, v);
		}
	}

	public static void main(String[] args) throws Exception {
		// 获取初始化配置
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf);
		
		job.setMapperClass(UserAvgRateMapper.class);
		job.setReducerClass(UserAvgRateReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Movie.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path("D:\\file\\test\\input.txt"));
		//FileOutputFormat.setOutputPath(job, new Path("D:\\file\\test\\output"));
		//outputformat输出到mysql中，自定义输出类
		job.setOutputFormatClass(MySQLTextOutputFormat.class);
		boolean b = job.waitForCompletion(true);
		
		System.exit(b?0:-1);

	}
}
