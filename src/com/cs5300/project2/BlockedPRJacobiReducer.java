package com.cs5300.project2;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class BlockedPRJacobiReducer extends Reducer<LongWritable, NodeWritable, NullWritable, Text>{

	private void AddReverseEdge(HashMap<Integer, List<Integer>> inReverseEdgeMapping, int src, int dst) {
		if(inReverseEdgeMapping.containsKey(dst) == false) {
			inReverseEdgeMapping.put(dst, new ArrayList<Integer>());
		}
		List<Integer> sources = inReverseEdgeMapping.get(dst);
		sources.add(src);
		inReverseEdgeMapping.put(dst, sources);
	}
	
	protected double IterateBlockOnce(
			HashMap<Integer, NodeWritable> inNodesInBlock, 
			HashMap<Integer, Double> inIncomingProbMass, 
			HashMap<Integer, List<Integer>> inReverseEdgeMapping) {
		HashMap<Integer, Double> nextPageRanks = new HashMap<Integer, Double>();
		
		double residual = 0.0;
		for(NodeWritable node: inNodesInBlock.values()) {
			
			/* Probability mass within the same block */
			double probMass = 0.0f;
			List<Integer> inComingNodesWithinBlock = inReverseEdgeMapping.containsKey(node.getNodeID()) ? inReverseEdgeMapping.get(node.getNodeID()) : new ArrayList<Integer>();
			
			for(int incomingNodeID: inComingNodesWithinBlock) {
				NodeWritable incomingNode = inNodesInBlock.get(incomingNodeID);
				probMass += (incomingNode.getCurrentPageRank() / (double)(incomingNode.getOutDegree()));
			}
			
			/* Probability mass from outside */
			probMass += inIncomingProbMass.containsKey(node.getNodeID()) ? inIncomingProbMass.get(node.getNodeID()): 0.0;
			
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
	
	private void ComputeBlockPageRank(HashMap<Integer, NodeWritable> inNodesInBlock, 
			HashMap<Integer, Double> inIncomingProbMass, 
			HashMap<Integer, List<Integer>> inReverseEdgeMapping,
			Reducer<LongWritable, NodeWritable, NullWritable, Text>.Context context) {
		HashMap<Integer, Double> startPageRanks = new HashMap<Integer, Double>();
		
		for(NodeWritable node: inNodesInBlock.values()) {
			startPageRanks.put(node.getNodeID(), node.getCurrentPageRank());
		}
		
		int iterations = 0;
		for(double blockResidual = 1.0f; blockResidual >= Constants.kInBlockResidualThreshold && iterations < Constants.kInBlockMaxkIterations; ++iterations) {
			blockResidual = IterateBlockOnce(inNodesInBlock, inIncomingProbMass, inReverseEdgeMapping);
		}
		
		double overallBlockResidual = 0.0;
		for(NodeWritable node: inNodesInBlock.values()) {
			overallBlockResidual += (Math.abs(startPageRanks.get(node.getNodeID()) - node.getCurrentPageRank()) / node.getCurrentPageRank());
		}
		
		/* Update hadoop counters */
		context.getCounter(BlockedPRRunnerBase.CounterType.RESIDUAL).increment((long)(overallBlockResidual*Math.pow(10, 5)));
		context.getCounter(BlockedPRRunnerBase.CounterType.BLOCKITERATIONCOUNTER).increment(iterations);
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
				double probMass = incomingProbMass.containsKey(n.getNodeID()) ? incomingProbMass.get(n.getNodeID()): 0.0;
				probMass += n.getDeltaPageRank();
				incomingProbMass.put(n.getNodeID(), probMass);
			}
		}
		
		/* Compute page rank for block by iterating over multiple times */
		ComputeBlockPageRank(nodesInBlock, incomingProbMass, reverseEdgeMapping, context);
		
		/* Emit data */
		for(NodeWritable node: nodesInBlock.values()) {
			context.write(NullWritable.get(), new Text(node.Serialize()));
		}
	}
}
