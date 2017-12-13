package org.webdatacommons.structureddata.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.webdatacommons.structureddata.tablegen.SparseMatrix.LongAggregationEntry;
import org.webdatacommons.structureddata.tablegen.SparseMatrix.SparseLongMatrix;

public class ItemsetTest {
	private static Logger log = Logger.getLogger(ItemsetTest.class);

	@Test
	public void maxFreqItemSetTest() {
		SparseLongMatrix<String> m = new SparseLongMatrix<String>();
		m.put("A", "B", 5L);
		m.put("B", "C", 4L);
		m.put("A", "C", 3L);
		m.put("A", "D", 2L);
		m.put("B", "D", 2L);
		m.put("C", "D", 2L);

		List<Map.Entry<Set<String>, LongAggregationEntry>> coocGroups = new ArrayList<Map.Entry<Set<String>, LongAggregationEntry>>(
				m.frequentItemsets(10, 0, 2).entrySet());
		Comparator<Map.Entry<Set<String>, LongAggregationEntry>> coocComp = new Comparator<Map.Entry<Set<String>, LongAggregationEntry>>() {
			@Override
			public int compare(
					Map.Entry<Set<String>, LongAggregationEntry> arg0,
					Map.Entry<Set<String>, LongAggregationEntry> arg1) {
				int sc = new Integer(arg0.getKey().size()).compareTo(arg1
						.getKey().size()) * -1;
				if (sc == 0) {
					return arg0.getValue().compareTo(arg1.getValue()) * -1;
				}
				return sc;
			}
		};
		Collections.sort(coocGroups, coocComp);
		for (Map.Entry<Set<String>, LongAggregationEntry> e : coocGroups) {
			log.info(e.getKey() + " - " + e.getValue().getSupport());

		}

	}
}
