package org.webdatacommons.structureddata.tablegen;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.log4j.Logger;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.webdatacommons.structureddata.tablegen.SparseMatrix.AggregationEntry;
import org.webdatacommons.structureddata.tablegen.SparseMatrix.AggregationMatrix;
import org.webdatacommons.structureddata.util.Statistics;
import org.webdatacommons.structureddata.util.Statistics.TopK;

public class StreamingStatsGenerator extends StreamingEntityReader {
	@SuppressWarnings("unused")
	private static Logger log = Logger.getLogger(StreamingStatsGenerator.class);

	public StreamingStatsGenerator(int capacity) {
		super(capacity);
	}

	private static final String rdftype = RDF.TYPE.toString();

	public static class PropertyAndDomainAggregationEntry implements
			AggregationEntry {

		private TopK topDomains;
		private long totalEntities;

		public PropertyAndDomainAggregationEntry(int topKSize) {
			topDomains = new TopK(topKSize);
			totalEntities = 0;
		}

		public PropertyAndDomainAggregationEntry(TopK intersectionDomains,
				long intersectionEntities) {
			this.topDomains = intersectionDomains;
			this.totalEntities = intersectionEntities;
		}

		@Override
		public int compareTo(AggregationEntry o) {
			PropertyAndDomainAggregationEntry ot = (PropertyAndDomainAggregationEntry) o;
			return new Long(getSupport()).compareTo(ot.getSupport());
		}

		@Override
		public long getSupport() {
			return totalEntities;
		}

		@Override
		public AggregationEntry intersect(AggregationEntry current,
				AggregationEntry... entries) {
			if (current == null) {
				return intersection(Arrays.asList(entries));
			} else {
				List<AggregationEntry> allEntries = new ArrayList<AggregationEntry>(
						Arrays.asList(entries));
				allEntries.add(current);
				return intersection(allEntries);
			}
		}

		private static AggregationEntry intersection(List<AggregationEntry> tops) {
			if (tops.size() < 2) {
				return tops.get(0);
			}
			long intersectionEntities = Long.MAX_VALUE;
			PropertyAndDomainAggregationEntry seed = (PropertyAndDomainAggregationEntry) tops
					.get(0);
			TopK intersectionDomains = new TopK(seed.getTopDomains().getLimit());

			for (Map.Entry<String, Long> e : seed.getTopDomains().getMap()
					.entrySet()) {
				intersectionDomains.add(e.getKey(), e.getValue());
			}

			for (int i = 1; i < tops.size(); i++) {
				if (tops.get(i) == null) {
					return null;
				}
				PropertyAndDomainAggregationEntry pde = (PropertyAndDomainAggregationEntry) tops
						.get(i);

				intersectionEntities = Math.min(intersectionEntities,
						pde.getTotalEntities());
				for (Map.Entry<String, Long> e : pde.getTopDomains().getMap()
						.entrySet()) {
					if (!intersectionDomains.getMap().containsKey(e.getKey())
							|| !pde.getTopDomains().getMap()
									.containsKey(e.getKey())) {
						intersectionDomains.getMap().remove(e.getKey());
					} else {
						intersectionDomains.getMap().put(
								e.getKey(),
								Math.min(
										intersectionDomains.getMap().get(
												e.getKey()),
										pde.getTopDomains().getMap()
												.get(e.getKey())));
					}
				}
			}
			return new PropertyAndDomainAggregationEntry(intersectionDomains,
					intersectionEntities);
		}

		public long getTotalEntities() {
			return totalEntities;
		}

		public TopK getTopDomains() {
			return topDomains;
		}

		public void increment(String domain) {
			topDomains.add(domain, 1);
			totalEntities++;
		}

		@Override
		public String toString() {
			return totalEntities + "/ " + topDomains.toString(10);
		}

	}

	public static class DomainPropertyCoocMatrix extends
			AggregationMatrix<String, PropertyAndDomainAggregationEntry> {

		private int topKLimit;

		public DomainPropertyCoocMatrix(int topKLimit) {
			this.topKLimit = topKLimit;
		}

		public void record(String row, String col, String domain) {
			// filter rdf:type, boring
			if (row.equals(rdftype) || col.equals(rdftype)) {
				return;
			}

			if (!has(row, col)) {
				put(row, col, new PropertyAndDomainAggregationEntry(topKLimit));
			}
			get(row, col).increment(domain);
		}
	}

	/*
	 * private final DomainPropertyCoocMatrix domPropCooc = new
	 * DomainPropertyCoocMatrix( 100);
	 */
	private final TopK topClasses = new TopK(1000);
	private final TopK topProperties = new TopK(1000);

	private long domainsRead = 0;
	private String currentDomain = "";

	private long totalProperties = 0;

	private long objectLinks = 0;
	private long objectLiterals = 0;
	private long objectRemoteLinks = 0;

	private boolean triggerEntity(Statement stmt) {
		return stmt.getPredicate().equals(RDF.TYPE);
	}

	@Override
	public void handle(Statement stmt) {
		if (triggerEntity(stmt)) {
			Entity e = resolveEntity(stmt.getSubject().toString(), 0);
			if (e != null) {
				// record type
				if (e.hasType()) {
					topClasses.increment(e.getType());
				}
				String entityDomain = Statistics.getDomain(e.getContext());

				if (e.getProperties() != null) {
					List<Entry<String, String>> entries = new ArrayList<Entry<String, String>>(
							e.getProperties().entrySet());
					for (int i = 0; i < entries.size(); i++) {
						/*
						 * for (int j = 0; j < i; j++) {
						 * domPropCooc.record(entries.get(i).getKey(), entries
						 * .get(j).getKey(), entityDomain); }
						 */
						totalProperties++;
						String propName = entries.get(i).getKey();
						// ignore rdf:type here
						if (propName.equals(rdftype)) {
							continue;
						}
						// record top properties

						topProperties.increment(propName);
						// record values
						String value = entries.get(i).getValue();
						if (value.startsWith("http")) {
							objectLinks++;
							if (!Statistics.getDomain(value).equals(
									entityDomain)) {
								objectRemoteLinks++;
							}
						} else {
							objectLiterals++;
						}
					}
				}

				if (!currentDomain.equals(entityDomain)) {
					currentDomain = entityDomain;
					domainsRead++;
				}
			}
		}
	}

	/*
	 * public DomainPropertyCoocMatrix getPropertiesByDomain() { return
	 * domPropCooc; }
	 */
	public TopK getTopClasses() {
		return topClasses;
	}

	public TopK getTopProperties() {
		return topProperties;
	}

	public long getDomainsRead() {
		return domainsRead;
	}

	public long getObjectLinks() {
		return objectLinks;
	}

	public long getObjectRemoteLinks() {
		return objectRemoteLinks;
	}

	public long getObjectLiterals() {
		return objectLiterals;
	}

	public long getTotalProperties() {
		return totalProperties;
	}
}
