package com.jacobromero.cs499_assignment03;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReviewedMovieMapper extends Mapper<LongWritable, Text, IntWritable, DoubleWritable>{
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] data = line.split(",");
		IntWritable i = new IntWritable(Integer.valueOf(data[0]));
		// pass the value of the double to the reducers
		DoubleWritable d = new DoubleWritable(Double.valueOf(data[2]));
		
		context.write(i, d);
	}
}
