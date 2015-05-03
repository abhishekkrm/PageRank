package com.cs5300.project2;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class BlockedPRMapper extends Mapper<LongWritable, Text, LongWritable, NodeWritable>{
@Override
protected void map(
		LongWritable key,
		Text value,
		Mapper<LongWritable, Text, LongWritable, NodeWritable>.Context context)
		throws IOException, InterruptedException {
	NodeWritable node = NodeWritable.Deserialize(value.toString());
	
	long nodeBlockID = BlockIDProvider.BlockIDofNode(node.getNodeID());
	context.write(new LongWritable(nodeBlockID), node);
	
	for(int neighbour: node.getNeighbours()) {
		long neighbourBlockID = BlockIDProvider.BlockIDofNode(neighbour);
		
		if(nodeBlockID != neighbourBlockID) {
			NodeWritable deltaPRNode = new NodeWritable();
		
			deltaPRNode.setDeltaPageRank(node.getCurrentPageRank()/node.getOutDegree());
			deltaPRNode.setNodeID(neighbour);
		
			context.write(new LongWritable(neighbourBlockID), deltaPRNode);
		}
	}
}
}
