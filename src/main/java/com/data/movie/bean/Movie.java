package com.data.movie.bean;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * 存储每行数据的
 * 
 * @author ThinkPad Movie 类要作为map端的value传输 实现序列化 Writable 序列化的接口
 * 
 */
public class Movie implements Writable {
	private String movie;
	private double rate;
	private String timeStamp;
	private int uid;

	public void set(String movie, double rate, String timeStamp, int uid) {
		this.movie = movie;
		this.rate = rate;
		this.timeStamp = timeStamp;
		this.uid = uid;
	}

	public String getMovie() {
		return movie;
	}

	public void setMovie(String movie) {
		this.movie = movie;
	}

	public double getRate() {
		return rate;
	}

	public void setRate(double rate) {
		this.rate = rate;
	}

	public String getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(String timeStamp) {
		this.timeStamp = timeStamp;
	}

	public int getUid() {
		return uid;
	}

	public void setUid(int uid) {
		this.uid = uid;
	}

	@Override
	public String toString() {
		return "Movie [movie=" + movie + ", rate=" + rate + ", timeStamp=" + timeStamp + ", uid=" + uid + "]";
	}

	/**
	 * 自定义的序列化方法
	 */
	@Override
	public void write(DataOutput out) throws IOException {
		// 写出去的类型和顺序
		out.writeUTF(movie);
		out.writeDouble(rate);
		out.writeUTF(timeStamp);
		out.writeInt(uid);
	}

	/**
	 * 自定义的反序列化方法
	 */

	@Override
	public void readFields(DataInput in) throws IOException {
		// 读取数据 按照写出去的顺序
		this.movie = in.readUTF();
		this.rate = in.readDouble();
		this.timeStamp = in.readUTF();
		this.uid = in.readInt();

	}

}
