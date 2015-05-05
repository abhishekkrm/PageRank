package com.cs5300.project2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;

abstract public class BlockedPRRunnerBase {
	public static enum CounterType {RESIDUAL, BLOCKITERATIONCOUNTER};
	
	abstract public void SetMapperClass(Job job);
	abstract public void SetReducerClass(Job job);
	
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length !=2) {
			System.err.println("Usage: BlockedPRRunner <input path> <outputpath>");
			System.exit(-1);
		}
		
		String inputFolder = args[0];
		String outputFolderName = args[1];
		
		List<Double> finalResidualValues = new ArrayList<Double>();
		List<Double> finalAvgIterations = new ArrayList<Double>();
		
		int exitCode = 0;
		double residualValue = 1.0;
		
		for(int iteration = 0; iteration < Constants.kBlockedPRMaxIterations && residualValue >= Constants.kOverallResidualThreshold; ++iteration) {
			Job job = Job.getInstance(new Configuration(), "block page rank");
				
			SetMapperClass(job);
			SetReducerClass(job);
						
			job.setJarByClass(PageRankCalculator.class);
			
			job.setMapOutputKeyClass(LongWritable.class);
			job.setMapOutputValueClass(NodeWritable.class);
			
			job.setOutputKeyClass(NullWritable.class);
			job.setOutputValueClass(Text.class);
			
			String outputFolder = outputFolderName + iteration;
			FileInputFormat.addInputPath((JobConf) job.getConfiguration(), new Path(inputFolder));
			FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(outputFolder));
			inputFolder = outputFolder;
			
			exitCode = job.waitForCompletion(true) ? 0 : 1;
			residualValue = (((double)(job.getCounters().findCounter(CounterType.RESIDUAL).getValue()))/Math.pow(10, 5)) / Constants.kNumNodes;
			
			long totalIterrations = job.getCounters().findCounter(CounterType.BLOCKITERATIONCOUNTER).getValue();
			double avgIterations = (double)totalIterrations/(double)Constants.kNumBlocks;
			
			finalResidualValues.add(residualValue);
			finalAvgIterations.add(avgIterations);
		}
		
		/* Print Results */
		for(int i = 0; i < finalResidualValues.size(); ++i) {
			System.out.println("Block PR: Residual Value for Iteration #" + i + " is: " + finalResidualValues.get(i));
			System.out.println("Block PR: Average number of per block iterations #" + i + " are: " + finalAvgIterations.get(i));
		}
		
		return exitCode;
	}
}
