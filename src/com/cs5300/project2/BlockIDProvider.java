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
	
	//Ref: https://gist.github.com/badboy/6267743
	private static long Hash64Shift(long key) {
		key = (~key) + (key << 21); 
		key = key ^ (key >>> 24);
		key = (key + (key << 3)) + (key << 8); 
		key = key ^ (key >>> 14);
		key = (key + (key << 2)) + (key << 4); 
		key = key ^ (key >>> 28);
		key = key + (key << 31);
		return key;
	}
	
	public static long BlockIDofNode(long inNodeID) {
		if(Constants.kUseRandomPartitioning) {
			return (Hash64Shift(inNodeID) % Constants.kNumBlocks);
		} else  {
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
}
