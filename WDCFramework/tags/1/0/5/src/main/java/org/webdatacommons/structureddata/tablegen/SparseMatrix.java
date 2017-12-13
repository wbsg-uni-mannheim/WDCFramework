package org.webdatacommons.structureddata.tablegen;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SparseMatrix<K, E> {

	private Map<K, Map<K, E>> rows = new HashMap<K, Map<K, E>>();

	public void put(K row, K col, E entry) {
		if (!rows.containsKey(row)) {
			rows.put(row, new HashMap<K, E>());
		}
		rows.get(row).put(col, entry);
	}

	public E get(K row, K col) {
		if (!rows.containsKey(row)) {
			return null;
		}
		if (!rows.get(row).containsKey(col)) {
			return null;
		}
		return rows.get(row).get(col);
	}

	public boolean has(K row, K col) {
		if (!rows.containsKey(row)) {
			return false;
		}
		if (!rows.get(row).containsKey(col)) {
			return false;
		}
		return rows.get(row).containsKey(col);
	}

	public boolean hasRow(K row) {
		return rows.containsKey(row);
	}

	public Set<Map.Entry<K, E>> getRow(K row) {
		return rows.get(row).entrySet();
	}

	public Set<SparseMatrix.Entry<K, E>> entries() {
		Set<SparseMatrix.Entry<K, E>> entrySet = new HashSet<SparseMatrix.Entry<K, E>>();
		for (Map.Entry<K, Map<K, E>> rowEntry : rows.entrySet()) {
			for (Map.Entry<K, E> colEntry : rowEntry.getValue().entrySet()) {
				entrySet.add(new SparseMatrix.Entry<K, E>(
						new SparseMatrix.Coord<K>(rowEntry.getKey(), colEntry
								.getKey()), colEntry.getValue()));
			}
		}
		return entrySet;
	}

	public static class Coord<K> {
		K row;
		K col;

		public Coord(K row, K col) {
			this.row = row;
			this.col = col;
		}

		@Override
		public int hashCode() {
			return row.hashCode() ^ col.hashCode();
		}

		public K getRow() {
			return row;
		}

		public K getCol() {
			return col;
		}

		@Override
		public String toString() {
			return "[" + row.toString() + ":" + col.toString() + "]";
		}
	}

	public static class Entry<K, E> {
		SparseMatrix.Coord<K> key;
		E value;

		public Entry(SparseMatrix.Coord<K> key, E value) {
			this.key = key;
			this.value = value;
		}

		public SparseMatrix.Coord<K> getKey() {
			return key;
		}

		public E getValue() {
			return value;
		}

		@Override
		public String toString() {
			return key.toString() + "= " + value.toString();
		}
	}

	public static class AggregationMatrix<K, E extends AggregationEntry>
			extends SparseMatrix<K, E> {

		@SuppressWarnings("unchecked")
		public Map<Set<K>, E> frequentItemsets(long transactions,
				double threshold, int minSize) {
			Map<Set<K>, E> itemsetSupport = new HashMap<Set<K>, E>();
			// seed map with all pairs from matrix
			for (SparseMatrix.Entry<K, E> entry : entries()) {
				if ((entry.getValue().getSupport() * 1.0) / transactions < threshold) {
					continue;
				}
				Set<K> newItemset = new HashSet<K>();
				newItemset.add(entry.getKey().getRow());
				newItemset.add(entry.getKey().getCol());
				itemsetSupport.put(newItemset, entry.getValue());
			}
			Map<Set<K>, E> newItemsetSupport = new HashMap<Set<K>, E>();
			Set<Set<K>> removeSets = new HashSet<Set<K>>();

			// try to build new groups as long as we find some
			int lengthLimit = 2;
			do {
				newItemsetSupport.clear();
				removeSets.clear();
				for (Map.Entry<Set<K>, E> curItemset : itemsetSupport
						.entrySet()) {
					// now for each group member, find its partners, check if
					// all other members in the group also have this partner,
					// calculate the minimum of all co-occurences, and add the
					// new group if the minimum is > minsupp

					if (curItemset.getKey().size() < lengthLimit) {
						continue;
					}

					for (K a : curItemset.getKey()) {
						if (!hasRow(a)) {
							continue;
						}
						for (Map.Entry<K, E> partner : getRow(a)) {
							K b = partner.getKey();
							E newEntry = null;

							for (K c : curItemset.getKey()) {
								if (a.equals(c)) {
									continue;
								}
								newEntry = (E) curItemset.getValue().intersect(
										newEntry, get(a, b), get(b, c),
										get(a, c));

								if (newEntry == null
										|| newEntry.getSupport() == 0) {
									break;
								}
							}
							if (newEntry == null) {
								continue;
							}
							if ((newEntry.getSupport() * 1.0 / transactions) > threshold) {
								Set<K> newItemset = new HashSet<K>();
								newItemset.addAll(curItemset.getKey());
								newItemset.add(b);

								if (!itemsetSupport.containsKey(newItemset)) {
									newItemsetSupport.put(newItemset, newEntry);
								}

								// remove subsets with equal support
								// -> maximal frequent itemsets
								List<K> newItL = new ArrayList<K>(newItemset);
								for (int i = 0; i < newItL.size(); i++) {
									Set<K> testSet = new HashSet<K>();
									if (i > 0) {
										testSet.addAll(newItL.subList(0, i));
									}
									if (i < newItL.size()) {
										testSet.addAll(newItL.subList(i + 1,
												newItL.size()));
									}
									if (itemsetSupport.containsKey(testSet)
											&& itemsetSupport.get(testSet)
													.getSupport() == newEntry
													.getSupport()) {
										removeSets.add(testSet);
									}
								}
							}
						}
					}

				}
				lengthLimit++;
				itemsetSupport.putAll(newItemsetSupport);
				for (Set<K> removeItemset : removeSets) {
					itemsetSupport.remove(removeItemset);
				}
			} while (newItemsetSupport.size() > 0);

			// remove all sets smaller than the given min size
			removeSets.clear();
			for (Map.Entry<Set<K>, E> e : itemsetSupport.entrySet()) {
				if (e.getKey().size() < minSize) {
					removeSets.add(e.getKey());
				}
			}
			for (Set<K> removeItemset : removeSets) {
				itemsetSupport.remove(removeItemset);
			}
			return itemsetSupport;
		}

		public List<Map.Entry<Set<K>, E>> getCoocSorted(long transactions,
				double threshold, int minSize) {
			List<Map.Entry<Set<K>, E>> coocSets = new ArrayList<Map.Entry<Set<K>, E>>(
					frequentItemsets(transactions, threshold, minSize)
							.entrySet());

			Comparator<Map.Entry<Set<K>, E>> coocComp = new Comparator<Map.Entry<Set<K>, E>>() {

				@Override
				public int compare(Map.Entry<Set<K>, E> o1,
						Map.Entry<Set<K>, E> o2) {
					/*
					 * int sc = new Integer(o1.getKey().size()).compareTo(o2
					 * .getKey().size()) * -1; if (sc == 0) {
					 */
					return o1.getValue().compareTo(o2.getValue()) * -1;
					/*
					 * } return sc;
					 */
				}
			};

			Collections.sort(coocSets, coocComp);
			return coocSets;
		}

		public List<SparseMatrix.Entry<K, E>> sortedEntries() {
			List<SparseMatrix.Entry<K, E>> entriesList = new ArrayList<SparseMatrix.Entry<K, E>>(
					entries());
			Collections.sort(entriesList,
					new AggregationEntryComparator<K, E>());
			return entriesList;
		}

		private static class AggregationEntryComparator<K, E extends AggregationEntry>
				implements Comparator<SparseMatrix.Entry<K, E>> {
			@Override
			public int compare(Entry<K, E> arg0, Entry<K, E> arg1) {
				return arg0.getValue().compareTo(arg1.getValue()) * -1;
			}
		}
	}

	public interface AggregationEntry extends Comparable<AggregationEntry> {
		public long getSupport();

		public AggregationEntry intersect(AggregationEntry current,
				AggregationEntry... entries);
	}

	public static class LongAggregationEntry implements AggregationEntry {

		private Long myLong;

		public LongAggregationEntry(Long l) {
			myLong = l;
		}

		@Override
		public int compareTo(AggregationEntry o) {
			return myLong.compareTo(o.getSupport());
		}

		@Override
		public long getSupport() {
			return myLong;
		}

		@Override
		public LongAggregationEntry intersect(AggregationEntry current,
				AggregationEntry... entries) {
			if (current == null) {
				return new LongAggregationEntry(min(entries));
			} else {
				return new LongAggregationEntry(Math.min(current.getSupport(),
						min(entries)));
			}
		}

		private long min(AggregationEntry... values) {
			long min = Long.MAX_VALUE;
			for (int i = 0; i < values.length; i++) {
				if (values[i] == null) {
					return 0;
				}
				LongAggregationEntry entryVal = (LongAggregationEntry) values[i];
				if (entryVal.getSupport() < min) {
					min = entryVal.getSupport();
				}
			}
			return min;
		}

		public void increment(Long increment) {
			myLong += increment;
		}

		public void increment() {
			increment(1L);
		}

		@Override
		public String toString() {
			return "" + myLong;
		}
	}

	public static class SparseLongMatrix<K> extends
			AggregationMatrix<K, LongAggregationEntry> {

		public void increment(K row, K col, long increment) {
			LongAggregationEntry val = get(row, col);
			if (val == null) {
				val = new LongAggregationEntry(increment);
				put(row, col, val);

			}
			val.increment(increment);
		}

		public void increment(K row, K col) {
			increment(row, col, 1);
		}

		public void put(K row, K col, long value) {
			super.put(row, col, new LongAggregationEntry(value));
		}

	}

	public static class SparseStringCooccurenceMatrix extends
			SparseLongMatrix<String> {
		public void record(String strA, String strB) {
			if (strA.compareTo(strB) < 0) {
				increment(strA, strB);
			} else {
				increment(strB, strA);
			}
		}
	}
}