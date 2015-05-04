package com.cs5300.project2;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PreProcessor {
	
	private boolean SelectEdge(double inEdgeSelectionProbability) {
		double fromNetID = 0.682; // ga286
		
		double rejectMin = 0.9 * fromNetID;
		double rejectLimit = rejectMin + 0.01;
		
		return ( ((inEdgeSelectionProbability >= rejectMin) && (inEdgeSelectionProbability < rejectLimit)) ? false : true );
	}
	
	private void AddEdge(Map<Integer, List<Integer>> inSelectedEdges, int src, int dst) {
		if(inSelectedEdges.containsKey(src) == false) {
			inSelectedEdges.put(src, new ArrayList<Integer>());
		}
		
		List<Integer> neighbours = inSelectedEdges.get(src);
		neighbours.add(dst);
		inSelectedEdges.put(src, neighbours);
	}
	
	private void WriteToFile(Set<Integer> inNodes, Map<Integer, List<Integer>> inSelectedEdges, String inOutputFile) {
		try {
			int totalNumberOfNodes = inNodes.size();
			double initialPageRank = 1.0 / (double)totalNumberOfNodes;
			
			BufferedWriter outputFileWriter = new BufferedWriter(new FileWriter(new File(inOutputFile)));
			for (Integer node: inNodes) {
				String nodeDescription = Integer.toString(node);
				nodeDescription = nodeDescription + ":" + Double.toString(initialPageRank);
				
				List<Integer> neighbours = inSelectedEdges.containsKey(node)?  inSelectedEdges.get(node): new ArrayList<Integer>();
				nodeDescription = nodeDescription + ":" + Integer.toString(neighbours.size());
				
				for(Integer neighbour: neighbours) {
					nodeDescription = nodeDescription + ":" + Integer.toString(neighbour);
				}
				outputFileWriter.write(nodeDescription + "\n");	
			}
			outputFileWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void ProcessEdgesFile(String inEdgeFileURL, String inOutputFile) {
		try {
			Map<Integer, List<Integer>> selectedEdges = new HashMap<Integer, List<Integer>>();
			Set<Integer> nodes = new HashSet<Integer>();
			
			BufferedReader edgeFileReader = new BufferedReader(new InputStreamReader(new URL(inEdgeFileURL).openStream()));
			String edge = null;
			
			while( (edge = edgeFileReader.readLine()) != null ) {
				String splited[] = edge.trim().split("\\s+");
				if(splited.length >= 3) {
					double edgeSelectionProbability = Double.parseDouble(splited[0]);
					int src = Integer.parseInt(splited[1]);
					int dst = Integer.parseInt(splited[2]);
					
					/* Need to retain all nodes */
					nodes.add(src);
					nodes.add(dst);
					
					/* Add edge */
					if(SelectEdge(edgeSelectionProbability) == true) {
						AddEdge(selectedEdges, src, dst);
					}
				}
			}
			WriteToFile(nodes, selectedEdges, inOutputFile);
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
