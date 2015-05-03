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

public class SingleNodeRunner {
	public static enum CounterType {RESIDUAL};
	
	public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		if(args.length !=2) {
			System.err.println("Usage: MaxTemperatureDriver <input path> <outputpath>");
			System.exit(-1);
		}
		
		Job job = Job.getInstance(new Configuration(), "single node page rank");
				
		job.setMapperClass(SingleNodeMapper.class);
		job.setReducerClass(SingleNodeReducer.class);
		
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(NodeWritable.class);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath((JobConf) job.getConfiguration(), new Path(args[0]));
		FileOutputFormat.setOutputPath((JobConf) job.getConfiguration(), new Path(args[1]));
		
		int exitCode = job.waitForCompletion(true) ? 0 : 1;
		double residualValue = ((double)(job.getCounters().findCounter(CounterType.RESIDUAL).getValue()))/Math.pow(10, 5);
		
		System.out.print(residualValue/5);
		
		return exitCode;
	}
}
