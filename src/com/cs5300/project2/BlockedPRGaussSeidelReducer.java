package com.cs5300.project2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BlockedPRGaussSeidelReducer extends BlockedPRJacobiReducer {
	@Override
	protected double IterateBlockOnce(
			Map<Integer, NodeWritable> inNodesInBlock,
			Map<Integer, Double> inIncomingProbMass,
			Map<Integer, List<Integer>> inReverseEdgeMapping) {
		HashMap<Integer, Double> nextPageRanks = new HashMap<Integer, Double>();
		
		double residual = 0.0;
		for(NodeWritable node: inNodesInBlock.values()) {
			
			/* Probability mass within the same block */
			double probMass = 0.0f;
			List<Integer> inComingNodesWithinBlock = inReverseEdgeMapping.containsKey(node.getNodeID()) ? inReverseEdgeMapping.get(node.getNodeID()) : new ArrayList<Integer>();
			
			for(int incomingNodeID: inComingNodesWithinBlock) {
				NodeWritable incomingNode = inNodesInBlock.get(incomingNodeID);
				/* Use updated page rank if possible */
				if(nextPageRanks.containsKey(incomingNodeID) ) {
					probMass += (nextPageRanks.get(incomingNodeID) / (double)(incomingNode.getOutDegree()));
				} else {
					probMass += (incomingNode.getCurrentPageRank() / (double)(incomingNode.getOutDegree()));
				}
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
}
