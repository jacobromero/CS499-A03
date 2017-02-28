package com.jacobromero.cs499_assignment03;

import java.io.File;
import java.util.HashMap;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.util.ToolRunner;

public class App extends Configured implements Tool {
	// static hashmaps for storing the top movies by avg rating, and top reviewers
	protected static HashMap<Integer, Double> movies = null;
	protected static HashMap<String, Double> moviesWithNames = new HashMap<String, Double>();
	protected static HashMap<Integer, Integer> topReviewers = null;
	
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new App(), args);
		System.exit(exitCode);
	}

	public int run(String[] args) throws Exception {
		// take in 4 arguments, 2 for the data inputs, 2 for data outputs
		if (args.length != 4) {
			System.err.printf("Usage: %s needs four arguments => TrainingRatings.txt movie_titles.txt {output_location_for_avg_movie_rating} {output_location_for_user_ratings} \n", getClass().getSimpleName());
			return -1;
		}
		
		// run 2 mapreduce jobs
		// job1 => gets average movie rating from TrainingRatings.txt 
		boolean job1Complete = this.runJob(App.class, "Average movie rating", args[0], args[2], IntWritable.class, DoubleWritable.class, TextOutputFormat.class, ReviewedMovieMapper.class, ReviewedMovieReducer.class);
		// job2 => counts the number of reviews each user has made
		boolean job2Complete = this.runJob(App.class, "User review count", args[0], args[3], IntWritable.class, IntWritable.class, TextOutputFormat.class, UserReviewMapper.class, UserReviewReducer.class);
		
		// after completing job1 we run another mapping job that will map all movie titles to their id
		if (job1Complete) {
			MovieNameMapper.run(args[1]);
			
			// when the movie mapper finishes we can print the title, and avg rating out
			if (moviesWithNames != null) {
				System.out.println("\n\nTop 10 movies by average rating.");
				for (String key : moviesWithNames.keySet()) {
					System.out.println(key + " | " + Math.round(moviesWithNames.get(key)*100.0)/100.0);
				}
			}
		}
		
		// when job2 finishes we can print out the top user reviewers
		if (job2Complete) {
			if (topReviewers != null) {
				System.out.println("\n\nTop 10 reviewers by number of reviews.");
				for (int key : topReviewers.keySet()) {
					System.out.println(key + " | " + topReviewers.get(key));
				}
			}
		}

		return (job1Complete && job2Complete) ? 0:1;
	}
	
	// extracted method to run a job
	private boolean runJob(Class mainClass, String jobName, String fileInputPath, String fileOutputPath, Class ouputKeyClass, Class outputValueClass, Class outputFormatClass, Class mapperClass, Class reducerClass) throws Exception {
		Job job = new Job();
		job.setJarByClass(mainClass);
		job.setJobName(jobName);
		FileInputFormat.addInputPath(job, new Path(fileInputPath));
		
		// try to delete the file if it already exists,
		// allows us to rerun the program without having to manual delete the same folder
		File f = new File(fileOutputPath);
		try {
			FileUtils.forceDelete(f);
		} catch (Exception e) {
			System.out.println(e);
		}
		FileOutputFormat.setOutputPath(job, new Path(fileOutputPath));
		job.setOutputKeyClass(ouputKeyClass);
		job.setOutputValueClass(outputValueClass);
		job.setOutputFormatClass(outputFormatClass);
		job.setMapperClass(mapperClass);
		job.setReducerClass(reducerClass);
		
		job.waitForCompletion(true);
		return job.isSuccessful();
	}
}
