package com.cs5300.project2;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.TreeMap;

public class BlockIDProvider {
	private static TreeMap<Long, Long> mBlockIDMap = null;
	
	private static void PopulateBlockIDmap() {
		try {
			mBlockIDMap = new TreeMap<Long, Long>();
			
			BufferedReader blockFileReader = new BufferedReader(new InputStreamReader(new URL(Constants.kBlockFileURL).openStream()));
			String block = null;
			
			long curBlockNumber = 0;
			long totalElements = 0;
			mBlockIDMap.put(totalElements, curBlockNumber);
			
			while( (block = blockFileReader.readLine()) != null ) {	
				curBlockNumber += 1;
				totalElements += Long.parseLong(block.trim());
				
				mBlockIDMap.put(totalElements, curBlockNumber);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static long BlockIDofNode(long inNodeID) {
		if(mBlockIDMap == null) {
			PopulateBlockIDmap();
		}
		
		if(mBlockIDMap.containsKey(inNodeID)) {
			return mBlockIDMap.get(inNodeID);
		} else {
			return mBlockIDMap.get(mBlockIDMap.lowerKey(inNodeID));
		}
	}
}
