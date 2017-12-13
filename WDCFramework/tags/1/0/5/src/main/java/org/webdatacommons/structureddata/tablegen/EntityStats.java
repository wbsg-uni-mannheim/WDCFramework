package org.webdatacommons.structureddata.tablegen;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;

public class EntityStats extends StreamingEntityReader {
	private static Logger log = Logger.getLogger(EntityStats.class);

	public EntityStats(int capacity) {
		super(capacity);
	}

	private Map<String, Long> propCounts = new HashMap<String, Long>();

	@Override
	public void handle(Statement s) {
		if (s.getPredicate().equals(RDF.TYPE)
				&& s.getObject().toString().equals("http://schema.org/Product")) {
			Entity e = resolveEntity(s.getSubject().toString(), 0);

			if (e.getProperties().size() > 0) {
				stats.addValue(e.getProperties().size());
				for (Entry<String, String> p : e.getProperties().entrySet()) {
					if (propCounts.containsKey(p.getKey())) {
						propCounts.put(p.getKey(),
								propCounts.get(p.getKey()) + 1);
					} else {
						propCounts.put(p.getKey(), 1L);
					}
				}
			}

			 log.info(e);
		}
	}
	
	private StatisticalDescription stats = new StatisticalDescription();

	public StatisticalDescription getStats() {
		stats.calculate();
		return stats;
	}
	
	public Map<String,Long> getCounts() {
		return propCounts;
	}

	public static void main(String[] args) throws RDFParseException,
			RDFHandlerException, FileNotFoundException, IOException {
		EntityStats es = new EntityStats(100);
		String f = "/Users/hannes/Desktop/wdc/ccrdf.html-microdata.75.nq.gz";
		es.addNquads(new InputStreamReader(new GZIPInputStream(
				new FileInputStream(f))));
		es.finish();
		System.out.println(es.getStats());
		printTop(es.getCounts());
	}

	public static void printTop(Map<String, Long> counts) {
		List<Entry<String, Long>> s = new ArrayList<Entry<String, Long>>(
				counts.entrySet());
		Collections.sort(s, new Comparator<Entry<String, Long>>() {
			@Override
			public int compare(Entry<String, Long> o1, Entry<String, Long> o2) {
				return o1.getValue().compareTo(o2.getValue()) * -1;
			}
		});

		for (Entry<String, Long> e : s) {
			if (e.getValue() < 2) {
				continue;
			}
			System.out.println(e.getKey() + ":\t\t" + e.getValue());
		}
	}

}
