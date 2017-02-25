package com.jacobromero.cs499_assignment03;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class App extends Configured implements Tool {
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new App(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		if (args.length != 3) {
			System.err.printf("Usage: %s needs three arguments, input and output files, for two job1s\n", getClass().getSimpleName());
			return -1;
		}

		Job job1 = new Job();
		Job job2 = new Job();
		
		job1.setJarByClass(App.class);
		job1.setJobName("Movie Review Average");
		job2.setJarByClass(App.class);
		job2.setJobName("User Review counts");

		FileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));

		job1.setOutputKeyClass(IntWritable.class);
		job1.setOutputValueClass(DoubleWritable.class);
		job1.setOutputFormatClass(TextOutputFormat.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		job1.setMapperClass(ReviewedMovieMapper.class);
		job1.setReducerClass(ReviewedMovieReducer.class);
		job2.setMapperClass(UserReviewMapper.class);
		job2.setReducerClass(UserReviewReducer.class);

		int returnValueJob1 = job1.waitForCompletion(true) ? 0:1;
		int returnValueJob2 = job2.waitForCompletion(true) ? 0:1;

		if (job1.isSuccessful()) {
			System.out.println("Job 1 was successful");
		} else if(!job1.isSuccessful()) {
			System.out.println("Job 1 was not successful");
		}
		
		if (job2.isSuccessful()) {
			System.out.println("Job 2 was successful");
		} else if(!job2.isSuccessful()) {
			System.out.println("Job 2 was not successful");
		}

		return (returnValueJob1 & returnValueJob2);
	}
}
