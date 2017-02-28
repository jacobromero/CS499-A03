package com.jacobromero.cs499_assignment03;

import java.io.IOException;
import java.util.HashMap;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class UserReviewReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	private HashMap<Integer, Integer> topValues = new HashMap<Integer, Integer>();
	
	@Override
	protected void reduce(IntWritable k, Iterable<IntWritable> values, Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException, InterruptedException {
		int count = 0;
		for (IntWritable v : values) {
			count += v.get();
		}
		
		// put the user into the top 10
		topValues.put(k.get(), count);
		
		// remove the smallest value from the hash map if we are over 10 users in the hash map
		if (topValues.size() > 10) {
			int key = getMin();
			if (key != -1) {
				topValues.remove(key);
			}
		}
		
		context.write(k, new IntWritable(count));
	}
	
	@Override
	protected void cleanup(Context context) {
		App.topReviewers = topValues;
	}
	
	// get the smallest item from the hash map
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
