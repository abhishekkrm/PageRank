package com.cs5300.project2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;


public class BlockedPRRunner {
	public static enum CounterType {RESIDUAL};
	
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length !=2) {
			System.err.println("Usage: BlockedPRRunner <input path> <outputpath>");
			System.exit(-1);
		}
		
		String inputFile = args[0];
		String outputFolder = args[1];
		
		int exitCode = 0;
		double residualValue = 1.0;
		for(int iteration = 0; iteration < Constants.kBlockedPRMaxIterations && residualValue >= Constants.kOverallResidualThreshold; ++iteration) {
			Job job = Job.getInstance(new Configuration(), "block page rank");
				
			job.setMapperClass(BlockedPRMapper.class);
			job.setReducerClass(BlockedPRReducer.class);
			
			job.setMapOutputKeyClass(IntWritable.class);
			job.setMapOutputValueClass(NodeWritable.class);
			
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			outputFolder = outputFolder + iteration;
			FileInputFormat.addInputPath((JobConf) job.getConfiguration(), new Path(inputFile));
			FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(outputFolder));
			inputFile = outputFolder + "/part-r-00000";
			
			exitCode = job.waitForCompletion(true) ? 0 : 1;
			residualValue = (((double)(job.getCounters().findCounter(CounterType.RESIDUAL).getValue()))/Math.pow(10, 5)) / Constants.kNumNodes;
			
			System.out.print("Block PR: Residual Value for Iteration #" + iteration + "is: " + residualValue);
		}
		return exitCode;
	}
}
