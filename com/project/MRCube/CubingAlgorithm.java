package com.project.MRCube;
/**
 * @author : Amit Patange
 * @Class : CubeAlgorithm
 * @Package : MRCube
 */


public abstract class CubingAlgorithm {
	
	Dataset data;
	Measure measure;
	int threshold;
	
	public CubingAlgorithm(Dataset data, Measure measure, int threshold) {
		this.data = data;
		this.measure = measure;
		this.threshold = threshold;
	}
	
	public abstract void materializeCube(ResultCollector result);
	
}
