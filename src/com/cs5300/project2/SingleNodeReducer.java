package com.cs5300.project2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SingleNodeReducer extends Reducer<IntWritable, NodeWritable, NullWritable, Text>{
@Override
protected void reduce(IntWritable key, Iterable<NodeWritable> values,
		Reducer<IntWritable, NodeWritable, NullWritable, Text>.Context context)
		throws IOException, InterruptedException {
	NodeWritable node = null;
	double sumDeltaPR = 0.0;
	
	for(NodeWritable n: values) {
		if(n.getIsNode()) {
			node = new NodeWritable(n);
		} else {
			sumDeltaPR += n.getDeltaPageRank();
		}
	}
	
	double nextPageRank = (1-Constants.kDampingFactor)/(Constants.kNumNodes) + Constants.kDampingFactor*sumDeltaPR;
	double residual = Math.abs(node.getmCurrentPageRank() - nextPageRank)/nextPageRank;
	
	/* Update counter */
	context.getCounter(SingleNodeRunner.CounterType.RESIDUAL).increment((long)(residual*Math.pow(10, 5)));
	
	/* Update page rank */
	node.setCurrentPageRank(nextPageRank);
	
	/* Emit data */
	context.write(NullWritable.get(), new Text(node.Serialize()));
}
}
