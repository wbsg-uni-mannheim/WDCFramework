package org.webdatacommons.webtables.extraction.model;
import java.util.Arrays;

import org.webdatacommons.webtables.tools.data.TableType;

/**
 * 
 * 
 * The code was mainly copied from the DWT framework 
 * (https://github.com/JulianEberius/dwtc-extractor & https://github.com/JulianEberius/dwtc-tools)
 * 
 * @author Robert Meusel (robert@informatik.uni-mannheim.de) - Translation to DPEF
 *
 */
public class ClassificationResult {
	private TableType resultType;
	private double[] distribution1, distribution2;
	
	public ClassificationResult(TableType tt, double[] dist1, double[] dist2) {
		this.resultType = tt;
		distribution1 = Arrays.copyOf(dist1, dist1.length);
		distribution2 = null;
		if (dist2 != null) {
			distribution2 = Arrays.copyOf(dist2, dist2.length);
		}
	}
	
	// returns the TableType as which the table has been
	// classified
	public TableType getTableType() {
		return resultType;
	}
	
	// returns classifier prediction distribution either for
	// phase 1 (binary) or phase 2 (non-layout, multiclass)
	public double[] getDistribution(int phase) {
		switch(phase) {
			case 1: return distribution1;
			case 2: return distribution2;
			default: throw new IllegalArgumentException("Phase must be either 1 or 2!");
		}
	}
	
	// for simple printing of the results
	public String toString() {
		return "Classified as: " + resultType.name()
				+ "\nDistribution for binary phase: " + Arrays.toString(distribution1)
			    + ((distribution2 == null) ? "" : "\nDistribution for second phase: " + Arrays.toString(distribution2))
			    + "\n(Distribution notation: [LAYOUT, RELATIONAL, ENTITY, MATRIX, OTHER])";
	}
}
