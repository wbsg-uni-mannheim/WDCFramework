package org.webdatacommons.structureddata.tablegen;

import java.util.List;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;

public class StatisticalDescription {
	public double min = 0;
	public double max = 0;
	public double mean = 0;
	public double median = 0;
	public long count = 0;

	private DescriptiveStatistics stats = new DescriptiveStatistics();

	public StatisticalDescription(List<Double> values) {
		if (values == null || values.size() == 0) {
			return;
		}
		for (Double v : values) {
			addValue(v);
		}
		calculate();
	}

	public StatisticalDescription() {
	}

	public void addValue(double value) {
		stats.addValue(value);
	}

	public StatisticalDescription calculate() {
		min = stats.getMin();
		max = stats.getMax();
		mean = stats.getMean();
		median = stats.getPercentile(50);
		count = stats.getN();
		return this;
	}

	@Override
	public String toString() {
		return "min=" + min + "\tmax=" + max + "\tmean=" + mean + "\tmedian=" + median + "\tcount="
				+ count;
	}
}