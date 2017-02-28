package com.jacobromero.cs499_assignment03;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.DoubleWritable;

public class ReviewedMovieReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {
	private HashMap<Integer, Double> topValues = new HashMap<Integer, Double>();
	
	@Override
	protected void reduce(IntWritable k, Iterable<DoubleWritable> values, Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable>.Context context) throws IOException, InterruptedException {
		double sum = 0;
		int count = 0;
		
		for (DoubleWritable v : values) {
			sum += v.get();
			count++;
		}
		
		// put into top 10 hash map
		topValues.put(k.get(), sum/count);
		
		// if we are over 10 movies in the hash map remove the smallest
		if (topValues.size() > 10) {
			int key = getMin();
			if (key != -1) {
				topValues.remove(key);
			}
		}
		

		context.write(k, new DoubleWritable(sum/count));
	}
	
	@Override
	protected void cleanup(Context context) {		
		App.movies = topValues;
	}
	
	// get the smallest value from the hash map
	private int getMin() {
		double min = Double.MAX_VALUE;
		int k = -1;
		for (int key : topValues.keySet()) {
			if (topValues.get(key) < min) {
				min = topValues.get(key);
				k = key;
			}
		}
		
		return k;
	}
}
