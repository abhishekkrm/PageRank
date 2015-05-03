package com.cs5300.project2;

public class Constants {
	public static double kDampingFactor = 0.85;
	public static int kNumNodes = 685230;
		
	public static double kInBlockResidualThreshold = 0.001f;
	public static double kOverallResidualThreshold = 0.001f;
	
	public static int kBlockedPRMaxIterations = 10;
	public static int kSingleNodePRMaxIterations = 6;
	
	public static String kBlockFileURL = "http://edu-cornell-cs-cs5300s15-proj2.s3.amazonaws.com/blocks.txt";
}
