package org.webdatacommons.webtables.extraction;

import java.io.IOException;
import java.util.List;

import org.jsoup.nodes.Document;

import org.webdatacommons.webtables.extraction.model.DocumentMetadata;
import org.webdatacommons.webtables.extraction.stats.StatsKeeper;
import org.webdatacommons.webtables.tools.data.Dataset;
/**
 * 
 * 
 * The code was mainly copied from the DWT framework (https://github.com/JulianEberius/dwtc-tools)
 * 
 * @author Robert Meusel (robert@informatik.uni-mannheim.de) - Translation to DPEF
 *
 */
public interface ExtractionAlgorithm {

	public abstract List<Dataset> extract(Document doc,
			DocumentMetadata metadata) throws IOException, InterruptedException;

	public abstract StatsKeeper getStatsKeeper();
	
}