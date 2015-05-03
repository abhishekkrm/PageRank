package com.cs5300.project2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

public class NodeWritable implements Writable{
	private double mCurrentPageRank = 0.0, mDeltaPageRank = 0.0;
	private int mNodeID = 0;
	private boolean mIsNode = true;
	private List<Integer> mNeighbours;
	
	/* Ctor */
	public NodeWritable() {
		mIsNode = true;
		mNeighbours = new ArrayList<Integer>();
	}
	
	/* Copy Ctor */
	public NodeWritable(NodeWritable node) {
		this();
		this.mNodeID = node.mNodeID;
		this.mCurrentPageRank = node.mCurrentPageRank;
		this.mDeltaPageRank = node.mDeltaPageRank;
		this.mIsNode = node.mIsNode;
		for(int neighbour: node.mNeighbours) {
			this.mNeighbours.add(neighbour);
		}
	}
	
	public double getCurrentPageRank() {
		return mCurrentPageRank;
	}

	public void setCurrentPageRank(double inCurrentPageRank) {
		this.mCurrentPageRank = inCurrentPageRank;
	}

	public int getNodeID() {
		return mNodeID;
	}

	public void setNodeID(int inNodeID) {
		this.mNodeID = inNodeID;
	}
	
	public void addNeighbour(int inNeighbour) {
		this.mNeighbours.add(inNeighbour);
	}
	
	public List<Integer> getNeighbours() {
		return mNeighbours;
	}
	
	public int getOutDegree() {
		return mNeighbours.size();
	}
	
	public void setDeltaPageRank(double inDeltaPageRank) {
		mIsNode = false;
		mDeltaPageRank = inDeltaPageRank;
	}
	
	public double getDeltaPageRank() {
		return mDeltaPageRank;
	}
	
	public boolean getIsNode() {
		return mIsNode;
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		/* Clear current content. Hadoop re-uses objects between calls to your map / reduce methods */
		/* Ref - http://stackoverflow.com/questions/13961833/how-to-pass-an-object-as-value-in-hadoop */
		mNeighbours.clear();
		
		mNodeID = arg0.readInt();
		mCurrentPageRank = arg0.readDouble();
		mDeltaPageRank = arg0.readDouble();
		mIsNode = arg0.readBoolean();
		
		int numNeighbours = arg0.readInt();
		for(int i = 0; i < numNeighbours; ++i) {
			mNeighbours.add(arg0.readInt());
		}
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		arg0.writeInt(mNodeID);
		arg0.writeDouble(mCurrentPageRank);
		arg0.writeDouble(mDeltaPageRank);
		arg0.writeBoolean(mIsNode);
		
		arg0.writeInt(mNeighbours.size());
		for(int neighbour: mNeighbours) {
			arg0.writeInt(neighbour);
		}
	}
	
	public static NodeWritable Deserialize(String inSerializedString) {
		NodeWritable node = new NodeWritable();
		
		String[] splitted = inSerializedString.split(":");
		
		node.setNodeID(Integer.parseInt(splitted[0]));
		node.setCurrentPageRank(Double.parseDouble(splitted[1]));
		
		int numNeighbours = Integer.parseInt(splitted[2]);
		for(int i = 0; i < numNeighbours; ++i) {
			node.addNeighbour(Integer.parseInt(splitted[i+3]));
		}
		
		return node;
	}
	
	public String Serialize() {
		String serializedNode = Integer.toString(mNodeID);
		serializedNode = serializedNode + ":" + Double.toString(mCurrentPageRank) + ":" + mNeighbours.size();
		
		for(int neighbour: mNeighbours) {
			serializedNode = serializedNode + ":" + Integer.toString(neighbour);
		}
		
		return serializedNode;
	}
}
