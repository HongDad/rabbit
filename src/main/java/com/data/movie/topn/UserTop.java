package com.data.movie.topn;


import com.data.movie.bean.Movie;
import com.google.gson.Gson;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * 每个人的评分的topN
 * @author ThinkPad
 *
 */
public class UserTop {
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
			}
		}

	}
	static class UserTopReducer extends Reducer<Text, Movie, Text, DoubleWritable> {

		@Override
		protected void reduce(Text uid, Iterable<Movie> iters, Reducer<Text, Movie, Text, DoubleWritable>.Context context)
				throws IOException, InterruptedException {
			List<Double> list = new ArrayList<>();
			for (Movie movie : iters) {
				double rate = movie.getRate();
				// 将每个人的所有的评分存储在list中
				list.add(rate) ;
			}
			// 对每个人的分数的集合进行降序排列
			Collections.sort(list, new Comparator<Double>() {

				@Override
				public int compare(Double o1, Double o2) {
					// TODO Auto-generated method stub
					return o2.compareTo(o1);
				}
			});
			
			// 输出
			for (int i = 0; i < Math.min(5, list.size()); i++) {
				context.write(uid, new DoubleWritable(list.get(i)));
			}
		}
	}
}
