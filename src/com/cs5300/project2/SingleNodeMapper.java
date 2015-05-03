package com.cs5300.project2;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SingleNodeMapper extends Mapper<LongWritable, Text, IntWritable, NodeWritable>{
@Override
protected void map(
		LongWritable key,
		Text value,
		Mapper<LongWritable, Text, IntWritable, NodeWritable>.Context context)
		throws IOException, InterruptedException {
	NodeWritable node = NodeWritable.Deserialize(value.toString());
	context.write(new IntWritable(node.getNodeID()), node);
	
	for(int neighbour: node.getNeighbours()) {
		NodeWritable deltaPRNode = new NodeWritable();
		deltaPRNode.setDeltaPageRank(node.getCurrentPageRank()/node.getOutDegree());
		context.write(new IntWritable(neighbour), deltaPRNode);
	}
}
}
