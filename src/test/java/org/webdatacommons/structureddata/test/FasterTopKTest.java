package org.webdatacommons.structureddata.test;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.log4j.Logger;
import org.junit.Test;
import org.webdatacommons.structureddata.util.Statistics.MapUtil;

import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;

public class FasterTopKTest {
	private static Logger log = Logger.getLogger(FasterTopKTest.class);

	public static class TopK {
		private int limit;

		private Map<String, Long> entries = new HashMap<String, Long>();
		
		Comparator<Map.Entry<String, Long>> entrySetComp = new Comparator<Map.Entry<String, Long>>() {
			@Override
			public int compare(Entry<String, Long> arg0,
					Entry<String, Long> arg1) {
				return arg0.getValue().compareTo(arg1.getValue());
			}
		};
		private SortedSet<Map.Entry<String, Long>> entrySet = new TreeSet<Map.Entry<String, Long>>(
				entrySetComp);

		public TopK(int limit) {
			this.limit = limit;
		}

		public void increment(String key) {
			add(key, 1);
		}

		// TODO: make faster!
		public void add(String key, long value) {
			while (entries.size() > limit) {
				Map.Entry<String, Long> smallestEntry = entrySet.last();
				entrySet.remove(smallestEntry);
				entries.remove(smallestEntry.getKey());
			}
			if (entries.containsKey(key)) {
				value += entries.get(key);
				
			}
			entries.put(key, value);
			
		}

		public Map<String, Long> getSortedMap() {
			Map<String, Long> sortedMap = MapUtil.sortByValue(entries);
			return sortedMap;
		}

		public Map<String, Long> getMap() {
			return entries;
		}

		public JSONArray toJsonArray() {
			JSONArray arr = new JSONArray();
			for (Entry<String, Long> entry : getSortedMap().entrySet()) {
				JSONObject entryJ = new JSONObject();
				try {
					entryJ.put("key", entry.getKey());
					// entryJ.put("abbrv", Statistics.abbrv(entry.getKey()));
					entryJ.put("count", entry.getValue());
				} catch (JSONException e) {
					log.warn(e);
				}
				arr.put(entryJ);
			}
			return arr;
		}

		public String toString() {
			return getSortedMap().toString();
		}

		public String toString(int limit) {
			String ret = "";
			for (Map.Entry<String, Long> e : getSortedMap().entrySet()) {
				ret += e.getKey() + ": " + e.getValue() + "\n";
				limit--;
				if (limit < 1) {
					break;
				}
			}
			return ret;
		}

		public int getLimit() {
			return limit;
		}
	}
	
	@Test
	public void fasterTopKTest() {
		@SuppressWarnings("unused")
		TopK t = new TopK(10);
		
	}
}
