package com.jacobromero.cs499_assignment03;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class UserReviewReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {

	@Override
	protected void reduce(IntWritable k, Iterable<IntWritable> values, Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable v : values) {
			count += v.get();
		}

		context.write(k, new IntWritable(count));
	}
}
