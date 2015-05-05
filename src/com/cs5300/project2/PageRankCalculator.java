package com.cs5300.project2;

public class PageRankCalculator {
	public static void main(String[] args) {
		try {
			//PreProcessor p = new PreProcessor();
			//p.ProcessEdgesFile("http://edu-cornell-cs-cs5300s15-proj2.s3.amazonaws.com/edges.txt", "output.txt");
			//new SingleNodeRunner().run(args);
			new BlockedPRJacobiRunner().run(args);
			//new BlockedPRGaussSeidelRunner().run(args);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
