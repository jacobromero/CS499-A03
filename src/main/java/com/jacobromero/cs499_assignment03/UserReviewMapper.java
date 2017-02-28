package com.jacobromero.cs499_assignment03;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class UserReviewMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable>{
	private static final IntWritable one = new IntWritable(1);
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] data = line.split(",");
		IntWritable i = new IntWritable(Integer.valueOf(data[1]));
		
		// for every review simply add 1 to the users total reviews
		IntWritable v = one;
		context.write(i, v);
	}
}
