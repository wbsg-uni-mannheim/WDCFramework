package org.webdatacommons.webtables.extraction;

import java.io.InputStream;

import org.jsoup.nodes.Element;

import org.webdatacommons.webtables.tools.data.TableType;
import org.webdatacommons.webtables.extraction.model.ClassificationResult;
import org.webdatacommons.webtables.extraction.model.FeaturesP1;
import org.webdatacommons.webtables.extraction.model.FeaturesP2;
import org.webdatacommons.webtables.extraction.util.TableConvert;

import weka.classifiers.Classifier;
import weka.core.Attribute;
import weka.core.Instance;

import com.google.common.base.Optional;

/**
 * 
 * 
 * The code was mainly copied from the DWT framework 
 * (https://github.com/JulianEberius/dwtc-extractor & https://github.com/JulianEberius/dwtc-tools)
 * 
 * @author Robert Meusel (robert@informatik.uni-mannheim.de) - Translation to DPEF
 *
 */
public class TableClassification {

	private TableConvert tableConvert;
	private FeaturesP1 phase1Features;
	private FeaturesP2 phase2Features;
	private Classifier classifier1;
	private Classifier classifier2;
	private Attribute classAttr1;
	private Attribute classAttr2;
	private double layoutVal, relationVal, entityVal, matrixVal, noneVal;

	// Constructor for classification class
	// initializes classificators and models
	// for phase 1 and 2
	public TableClassification(String phase1ModelPath, String phase2ModelPath) {
		tableConvert = new TableConvert(2, 2);
		phase1Features = new FeaturesP1();
		phase2Features = new FeaturesP2();
		try {
			classifier1 = loadModelFromClasspath(phase1ModelPath);
			classifier2 = loadModelFromClasspath(phase2ModelPath);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// the order of the FastVector elements has
		// to be the same as in the ARFF metadata
		// of the ARFF file which has been used
		// to train the model

		// Phase 1
		classAttr1 = new Attribute("class", phase1Features.getClassVector());
		layoutVal = classAttr1.indexOfValue("LAYOUT");

		// Phase 2
		classAttr2 = new Attribute("class", phase2Features.getClassVector());

		relationVal = classAttr2.indexOfValue("RELATION");
		entityVal = classAttr2.indexOfValue("ENTITY");
		matrixVal = classAttr2.indexOfValue("MATRIX");
		noneVal = classAttr2.indexOfValue("NONE");
	}

	// Returns classification as TableType from given
	// JSoup.Element 'table'
	// Constructor of this class has to be called first
	public ClassificationResult classifyTable(Element table) {
		Optional<Element[][]> convertedTable = tableConvert.toTable(table);
		if (!convertedTable.isPresent()) {
			// convert failed because of malformed table -> LAYOUT
			double[] dist1 = new double[] { 1.0, 0.0, 0.0, 0.0, 0.0 };
			return new ClassificationResult(TableType.LAYOUT, dist1, null);
		}
		return classifyTable(convertedTable.get());
	}

	// Returns classification as TableType from given
	// JSoup.Element 'table'
	// Constructor of this class has to be called first
	public ClassificationResult classifyTable(Element[][] convertedTable) {
		double[] dist1, dist2;
		Instance currentInst = phase1Features.computeFeatures(convertedTable);
		try {
			double cls = classifier1.classifyInstance(currentInst);
			dist1 = classifier1.distributionForInstance(currentInst);
			if (cls == layoutVal) {
				return new ClassificationResult(TableType.LAYOUT, dist1, null);
			} else {
				currentInst = phase2Features.computeFeatures(convertedTable);
				cls = classifier2.classifyInstance(currentInst);
				dist2 = classifier2.distributionForInstance(currentInst);
				// classifier2.distributionForInstance(instance)
				TableType resultType;
				if (cls == relationVal)
					resultType = TableType.RELATION;
				else if (cls == entityVal)
					resultType = TableType.ENTITY;
				else if (cls == matrixVal)
					resultType = TableType.MATRIX;
				else if (cls == noneVal)
					resultType = TableType.OTHER;
				else {
					// Error
					resultType = TableType.LAYOUT;
				}
				return new ClassificationResult(resultType, dist1, dist2);
			}
		} catch (Exception e) {
			// classification failed
			e.printStackTrace();
			return null;
		}

	}

	public static Classifier loadModelFromFile(String path) throws Exception {
		return (Classifier) weka.core.SerializationHelper.read(path);
	}

	public static Classifier loadModelFromClasspath(String path)
			throws Exception {
		InputStream stream = TableClassification.class
				.getResourceAsStream(path);
		return (Classifier) weka.core.SerializationHelper.read(stream);
	}
}
