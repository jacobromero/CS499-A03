package com.jacobromero.cs499_assignment03;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieNameMapper extends Mapper<LongWritable, Text, Text, Text>{
	// extracted run job command
	protected static void run(String inputPath) throws Exception {
		Job getMovieNames = new Job();
		getMovieNames.setJarByClass(App.class);
		getMovieNames.setJobName("Movie ids to Names");
		FileInputFormat.addInputPath(getMovieNames, new Path(inputPath));
		
		// set temporary output file,
		// we dont care about keeping the data
		FileOutputFormat.setOutputPath(getMovieNames, new Path("tmpFile"));
		getMovieNames.setOutputKeyClass(Text.class);
		getMovieNames.setOutputValueClass(DoubleWritable.class);
		getMovieNames.setMapperClass(MovieNameMapper.class);
		// set no reduce jobs, all we want is to map the names to the top 10 movies
		getMovieNames.setNumReduceTasks(0);
		getMovieNames.waitForCompletion(true);
		
		// remove tmpfile
		File f1 = new File("tmpFile");
		try {
			FileUtils.forceDelete(f1);
		} catch (Exception e) {
			// oh well
		}
	}
	
	@Override
	protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, Text, Text>.Context context) throws IOException, InterruptedException {
		String line = value.toString();
		String[] data = line.split(",");
		int movieKey = Integer.parseInt(data[0]);
		String movieName = data[2];
		
		// if the movie id is in the top 10 then save the name
		if (App.movies.get(movieKey) != null) {
			App.moviesWithNames.put(movieName, App.movies.get(movieKey));
		}
	}
}
