package com.jacobromero.cs499_assignment03;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;

public class ReviewedMovieReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

	@Override
	protected void reduce(IntWritable k, Iterable<DoubleWritable> values, Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {

		double sum = 0;
		int count = 0;
		for (DoubleWritable v : values) {
			sum += v.get();
			count++;
		}

		context.write(k, new DoubleWritable(sum/count));
	}

}
