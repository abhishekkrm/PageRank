package com.cs5300.project2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

public class SingleNodeRunner {
	public static enum CounterType {RESIDUAL};
	
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length !=2) {
			System.err.println("Usage: SingleNodeRunner <input path> <outputpath>");
			System.exit(-1);
		}
		
		String inputFolder = args[0];
		String outputFolderName = args[1];
		
		List<Double> finalResidualValues = new ArrayList<Double>();
		
		int exitCode = 0;
		double residualValue = 1.0;
		
		for(int iteration = 0; iteration < Constants.kSingleNodePRMaxIterations && residualValue >= Constants.kOverallResidualThreshold; ++iteration) {
			Job job = Job.getInstance(new Configuration(), "single node page rank");
				
			job.setMapperClass(SingleNodeMapper.class);
			job.setReducerClass(SingleNodeReducer.class);
			
			job.setJarByClass(PageRankCalculator.class);
			
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(NodeWritable.class);
			
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			String outputFolder = outputFolderName + iteration;
			FileInputFormat.addInputPath((JobConf) job.getConfiguration(), new Path(inputFolder));
			FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(outputFolder));
			inputFolder = outputFolder;
			
			exitCode = job.waitForCompletion(true) ? 0 : 1;
			residualValue = (((double)(job.getCounters().findCounter(CounterType.RESIDUAL).getValue()))/Constants.kCounterMultiplier)/Constants.kNumNodes;
			
			finalResidualValues.add(residualValue);
		}
		
		for(int i = 0; i < finalResidualValues.size(); ++i) {
			System.out.println("Single Node PR: Residual Value for Iteration #" + i + " is: " + finalResidualValues.get(i));
		}
		
		return exitCode;
	}
}
