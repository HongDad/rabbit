package com.data.movie.topn;


import com.data.movie.bean.Movie;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class TopNJob {
	/**
	 * 按照uid为key输出
	 * 
	 * @author ThinkPad
	 *
	 */
	static class TopNMapper extends Mapper<LongWritable, Text, Text, Movie> {
		Gson gs = new Gson();
		Text k = new Text();
		Movie v = null;

		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Movie>.Context context)
				throws IOException, InterruptedException {
			try {
				String line = value.toString();
				v = gs.fromJson(line, Movie.class);
				k.set(v.getUid() + "");
				context.write(k, v);




			} catch (Exception e) {
				// TODO Auto-generated catch block
				// e.printStackTrace();
			}
		}
	}

	/**
	 * uid 和 评论的次数
	 *
	 * @author ThinkPad
	 *
	 */
	static class TopNReducer extends Reducer<Text, Movie, Text, IntWritable> {
		// uid 次数
		Map<String, Integer> map = new HashMap<>();

		@Override
		protected void reduce(Text key, Iterable<Movie> iters, Reducer<Text, Movie, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int count = 0;
			for (Movie movie : iters) {
				count++;
			}
			map.put(key.toString(), count);
		}
		// 对map排序 --> 输出

		/**
		 * 只在reduce方法执行完后执行一次
		 */
		@Override
		protected void cleanup(Reducer<Text, Movie, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			// 对map排序 list
			Set<Entry<String, Integer>> set = map.entrySet();
			// 将set转换成list
			ArrayList<Entry<String, Integer>> list = new ArrayList<>(set);

			Collections.sort(list, new Comparator<Entry<String, Integer>>() {
				@Override
				public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
					// TODO Auto-generated method stub
					return o2.getValue() - o1.getValue();
				}
			});
			// 输出 输出topn 5
			// int max = Math.max(1, 10) ; 返回两个数的大值
			// int min = Math.min(12, 2); 返回两个数的小值
			for (int i = 0; i < Math.min(5, list.size()); i++) {
				Entry<String, Integer> entry = list.get(i);
				context.write(new Text(entry.getKey()), new IntWritable(entry.getValue()));
			}

		}

	}

	public static void main(String[] args) throws Exception {
		// 初始化配置信息
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		// 设置 mapreduce的两个类
		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);
		// map的输出类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Movie.class);
		// 最终的输出类型(reduce的输出类型)
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		// 设置reducetask的数量 默认1个
		// job.setNumReduceTasks(2); //local1944506709_0001_r_000001_0
		// 处理的数据的路径
		FileInputFormat.setInputPaths(job, new Path("D:\\data\\movie\\input"));
		// 结果的输出
		FileOutputFormat.setOutputPath(job, new Path("D:\\data\\movie\\topN2"));
		// 提交 任务 并打印执行日志
		// 返回值是标识任务执行是否成功 成功true
		boolean flag = job.waitForCompletion(true);
		// 校验程序是否正常退出
		System.exit(flag ? 0 : -1);

	}
}
