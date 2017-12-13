package org.webdatacommons.webtables.extraction.stats;

import java.util.Map;

/* Abstract interface for collecting stats during the extraction process. 
 * Used to be implemented with Hadoop Counters, current implementation just uses
 * a HashMap which is later written to Amazon SimpleDB.
 */
/**
 * 
 * 
 * The code was mainly copied from the DWT framework 
 * (https://github.com/JulianEberius/dwtc-extractor & https://github.com/JulianEberius/dwtc-tools)
 * 
 * @author Robert Meusel (robert@informatik.uni-mannheim.de) - Translation to DPEF
 *
 */
public abstract class StatsKeeper {

	public abstract void incCounter(Enum<?> counter);

	public abstract Map<String, Integer> statsAsMap();

	public abstract void reportProgress();


	public static class NullStats extends StatsKeeper {

		@Override
		public void incCounter(Enum<?> counter) {
		}

		@Override
		public void reportProgress() {
		}

		@Override
		public Map<String, Integer> statsAsMap() {
			return null;
		}

	}
}
