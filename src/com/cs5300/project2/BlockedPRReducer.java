package com.cs5300.project2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BlockedPRReducer extends Reducer<LongWritable, NodeWritable, NullWritable, Text>{

	private void AddReverseEdge(HashMap<Integer, List<Integer>> inReverseEdgeMapping, int src, int dst) {
		if(inReverseEdgeMapping.containsKey(dst) == false) {
			inReverseEdgeMapping.put(dst, new ArrayList<Integer>());
		}
		List<Integer> sources = inReverseEdgeMapping.get(dst);
		sources.add(src);
		inReverseEdgeMapping.put(dst, sources);
	}
	
	private double IterateBlockOnce(HashMap<Integer, NodeWritable> inNodesInBlock, HashMap<Integer, Double> inIncomingProbMass, HashMap<Integer, List<Integer>> inReverseEdgeMapping) {
		HashMap<Integer, Double> nextPageRanks = new HashMap<Integer, Double>();
		
		for(NodeWritable node: inNodesInBlock.values()) {
			nextPageRanks.put(node.getNodeID(), 0.0);
		}
		
		double residual = 0.0;
		for(NodeWritable node: inNodesInBlock.values()) {
			
			/* Probability mass within the same block */
			double probMass = nextPageRanks.get(node.getNodeID());
			List<Integer> inComingNodesWithinBlock = inReverseEdgeMapping.getOrDefault(node.getNodeID(), new ArrayList<Integer>());
			
			for(int incomingNodeID: inComingNodesWithinBlock) {
				NodeWritable incomingNode = inNodesInBlock.get(incomingNodeID);
				probMass += (incomingNode.getCurrentPageRank() / (double)(incomingNode.getOutDegree()));
			}
			
			/* Probability mass from outside */
			probMass += inIncomingProbMass.getOrDefault(node.getNodeID(), 0.0);
			
			/* updated page rank of this node */
			double updatedPageRank = Constants.kDampingFactor*probMass + (1-Constants.kDampingFactor)/Constants.kNumNodes;
			nextPageRanks.put(node.getNodeID(), updatedPageRank);
			
			residual += (Math.abs(node.getCurrentPageRank() - updatedPageRank) / updatedPageRank);
		}
		
		for(int nodeID: nextPageRanks.keySet()) {
			NodeWritable n = inNodesInBlock.get(nodeID);
			n.setCurrentPageRank(nextPageRanks.get(nodeID));
			inNodesInBlock.put(nodeID, n);
		}
		
		return (residual / inNodesInBlock.size());
	}
	
	private double ComputeBlockPageRank(HashMap<Integer, NodeWritable> inNodesInBlock, HashMap<Integer, Double> inIncomingProbMass, HashMap<Integer, List<Integer>> inReverseEdgeMapping) {
		HashMap<Integer, Double> startPageRanks = new HashMap<Integer, Double>();
		
		for(NodeWritable node: inNodesInBlock.values()) {
			startPageRanks.put(node.getNodeID(), node.getCurrentPageRank());
		}
		
		for(double blockResidual = 1.0f; blockResidual < Constants.kInBlockResidualThreshold; blockResidual = IterateBlockOnce(inNodesInBlock, inIncomingProbMass, inReverseEdgeMapping));
		
		double overallBlockResidual = 0.0;
		for(NodeWritable node: inNodesInBlock.values()) {
			overallBlockResidual += (Math.abs(startPageRanks.get(node.getNodeID()) - node.getCurrentPageRank()) / node.getCurrentPageRank());
		}
		
		return overallBlockResidual;
	}
	
	@Override
	protected void reduce(LongWritable key, Iterable<NodeWritable> values,
			Reducer<LongWritable, NodeWritable, NullWritable, Text>.Context context)
			throws IOException, InterruptedException {
		
		HashMap<Integer, NodeWritable> nodesInBlock = new HashMap<Integer, NodeWritable>();
		HashMap<Integer, Double> incomingProbMass = new HashMap<Integer, Double>();
		HashMap<Integer, List<Integer>> reverseEdgeMapping = new HashMap<Integer, List<Integer>>();
		
		for(NodeWritable n: values) {
			if(n.getIsNode()) {
				nodesInBlock.put(n.getNodeID(), new NodeWritable(n));
				for(int neighbour: n.getNeighbours()) {
					AddReverseEdge(reverseEdgeMapping, n.getNodeID(), neighbour);
				}
			} else {
				double probMass = incomingProbMass.getOrDefault(n.getNodeID(), 0.0);
				probMass += n.getDeltaPageRank();
				incomingProbMass.put(n.getNodeID(), probMass);
			}
		}
		
		/* Compute page rank for block by iterating over multiple times */
		double overallBlockResidual = ComputeBlockPageRank(nodesInBlock, incomingProbMass, reverseEdgeMapping);
		
		/* UPdate hadoop counter and emit data */
		context.getCounter(BlockedPRRunner.CounterType.RESIDUAL).increment((long)(overallBlockResidual*Math.pow(10, 5)));
		
		for(NodeWritable node: nodesInBlock.values()) {
			context.write(NullWritable.get(), new Text(node.Serialize()));
		}
	}
}
