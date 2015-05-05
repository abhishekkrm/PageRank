package com.cs5300.project2;

import org.apache.hadoop.mapreduce.Job;

public class BlockedPRJacobiRunner extends BlockedPRRunnerBase {

	@Override
	public void SetMapperClass(Job job) {
		job.setMapperClass(BlockedPRMapper.class);
	}

	@Override
	public void SetReducerClass(Job job) {
		job.setReducerClass(BlockedPRJacobiReducer.class);
	}

}
