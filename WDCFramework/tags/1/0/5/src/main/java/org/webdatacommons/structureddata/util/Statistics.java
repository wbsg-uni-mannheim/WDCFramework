package org.webdatacommons.structureddata.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Writer;
import java.net.URL;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import org.apache.any23.rdf.Prefixes;
import org.apache.commons.collections15.map.LRUMap;
import org.apache.log4j.Logger;
import org.webdatacommons.framework.io.CSVExport;
import org.webdatacommons.framework.io.CSVReaderSingleLine;
import org.webdatacommons.structureddata.extractor.RDFExtractor;
import org.webdatacommons.structureddata.tablegen.TripleStats;

import au.com.bytecode.opencsv.CSVReader;

import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.amazonaws.util.json.JSONTokener;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;

public class Statistics {
	private static Logger log = Logger.getLogger(Statistics.class);

	public static final DecimalFormat twoDForm = new DecimalFormat("#.##");

	public static final DecimalFormat largeNumberFormat = new DecimalFormat(
			",###");

	private static class ContinousAverage {
		private double sum = 0;
		private double average = 0;

		public void add(Number newValue) {
			average = (average * sum + newValue.doubleValue()) / (sum + 1);
			sum++;
		}

		public Number getAverage() {
			return average;
		}

		public String toString() {
			return twoDForm.format(getAverage());
		}
	}

	public static Map<String, Integer> getHeader(CSVReader reader)
			throws IOException {
		String[] header = reader.readNext();
		if (header == null) {
			throw new IOException("No CSV header found.");
		}
		Map<String, Integer> k = new HashMap<String, Integer>();
		for (int i = 0; i < header.length; i++) {
			k.put(header[i], i);
		}
		return k;
	}

	public static JSONObject readDataStat(Reader csvReader) throws IOException {
		CSVReader reader = new CSVReaderSingleLine(csvReader);
		Map<String, Integer> k = getHeader(reader);
		String[] line;
		long pages = 0;
		long parsed = 0;
		long triples = 0;
		long size = 0;
		long time = 0;
		ContinousAverage rateAvg = new ContinousAverage();

		while ((line = reader.readNext()) != null) {
			pages += Long.parseLong(line[k.get("pagesTotal")]);
			parsed += Long.parseLong(line[k.get("pagesParsed")]);
			triples += Long.parseLong(line[k.get("pagesTriples")]);
			size += Long.parseLong(line[k.get("size")]);
			time += Double.parseDouble(line[k.get("duration")]);
			double rate = Double.parseDouble(line[k.get("rate")]);
			if (Double.isNaN(rate) || Double.isInfinite(rate)) {
				log.warn(Arrays.asList(line));
			} else {
				rateAvg.add(rate);
			}
		}
		JSONObject res = new JSONObject();

		try {
			res.put("pagesTotal", pages);
			res.put("pagesParsed", parsed);
			res.put("pagesWithTriples", triples);
			res.put("totalBytes", size);
			res.put("cpuHours", time / 60);
			res.put("averagePagesPerSecond", rateAvg.getAverage());
			res.put("totalSize", CSVExport.humanReadableByteCount(size, false));

		} catch (JSONException e) {
			log.warn("Failed to generate JSON", e);
		}
		return res;
	}

	public static JSONObject readPageStat(Reader csvReader) throws IOException {

		CSVReader reader = new CSVReaderSingleLine(csvReader, '\t');
		Map<String, Integer> k = getHeader(reader);

		Map<String, Long> triplesPerFormat = new HashMap<String, Long>();
		Map<String, Long> urlsPerFormat = new HashMap<String, Long>();
		final Map<String, Long> sldsPerFormat = new HashMap<String, Long>();
		Map<String, ContinousAverage> avgTriplesPerUrlPerFormat = new HashMap<String, ContinousAverage>();
		final Map<String, ContinousAverage> avgTriplesPerSldPerFormat = new HashMap<String, ContinousAverage>();
		final Map<String, TopK> topDomainsPerFormat = new HashMap<String, TopK>();
		final Map<String, TopK> topDomainsPerFormatUrls = new HashMap<String, TopK>();

		final TopK topDomains = new TopK(10000);
		final TopK topDomainsUrls = new TopK(10000);

		Map<String, Map<String, Long>> triplesPerSLD = (new LRUMap<String, Map<String, Long>>(
				1000) {
			private static final long serialVersionUID = 1L;

			@Override
			protected boolean removeLRU(
					LinkEntry<String, Map<String, Long>> entry) {
				// add result to global statistics, we assume no more
				// triples from this domain after now
				for (String ex : RDFExtractor.EXTRACTORS) {
					Long value = entry.getValue().get(ex);
					if (value == null || value < 1) {
						continue;
					}
					add(sldsPerFormat, ex, 1L);
					avgTriplesPerSldPerFormat.get(ex).add(value);
					topDomainsPerFormat.get(ex).add(entry.getKey(), value);
					topDomains.add(entry.getKey(), value);
				}
				return true;
			}
		});

		// init maps
		for (String ex : RDFExtractor.EXTRACTORS) {
			triplesPerFormat.put(ex, 0L);
			urlsPerFormat.put(ex, 0L);
			sldsPerFormat.put(ex, 0L);
			avgTriplesPerUrlPerFormat.put(ex, new ContinousAverage());
			avgTriplesPerSldPerFormat.put(ex, new ContinousAverage());
			topDomainsPerFormat.put(ex, new TopK(1000));
			topDomainsPerFormatUrls.put(ex, new TopK(1000));

		}

		String[] line;
		long totalTriples = 0;
		long totalBytes = 0;
		long lines = 0;
		@SuppressWarnings("unused")
		long errors = 0;

		long minDate = Long.MAX_VALUE;
		long maxDate = 0;

		while ((line = reader.readNext()) != null) {
			// skip empty lines
			if (line.length < k.keySet().size()) {
				continue;
			}
			lines++;
			if (lines % 100000 == 0) {
				log.info(lines);
			}
			try {
				// skip entries where no triples were found
				long triplesFound = Long.parseLong(line[k.get("totalTriples")]);
				long metaTriples = Long
						.parseLong(line[k.get("html-head-meta")]);
				if ((triplesFound - metaTriples) < 1) {
					continue;
				}
				totalTriples += triplesFound;

				long timestamp = Long.parseLong(line[k.get("timestamp")]);
				if (timestamp < minDate) {
					minDate = timestamp;
				}
				if (timestamp > maxDate) {
					maxDate = timestamp;
				}

				totalBytes += Long.parseLong(line[k.get("recordLength")]);

				URL url = new URL(line[k.get("uri")]);
				String domain = getDomain(url.getHost());
				topDomainsUrls.increment(domain);

				// triples per format
				for (String ex : RDFExtractor.EXTRACTORS) {
					long triplesForExtractor = Long.parseLong(line[k.get(ex)]);
					if (triplesForExtractor < 1) {
						continue;
					}

					add(urlsPerFormat, ex, 1L);
					add(triplesPerFormat, ex, triplesForExtractor);

					if (!triplesPerSLD.containsKey(domain)) {
						triplesPerSLD.put(domain, new HashMap<String, Long>());
					}
					add(triplesPerSLD.get(domain), ex, triplesForExtractor);

					avgTriplesPerUrlPerFormat.get(ex).add(triplesForExtractor);
					topDomainsPerFormatUrls.get(ex).increment(domain);
				}

			} catch (Exception e) {
				log.warn("Parsing error " + e.getMessage());
				errors++;
			}
		}

		// do statistics for stuff remaining in lru cache
		for (Map.Entry<String, Map<String, Long>> entry : triplesPerSLD
				.entrySet()) {
			for (String ex : RDFExtractor.EXTRACTORS) {
				Long value = entry.getValue().get(ex);
				if (value == null || value < 1) {
					continue;
				}
				add(sldsPerFormat, ex, 1L);
				avgTriplesPerSldPerFormat.get(ex).add(value);
				topDomainsPerFormat.get(ex).add(entry.getKey(), value);
				topDomains.add(entry.getKey(), value);
			}
		}

		JSONObject resultData = new JSONObject();

		try {
			for (String ex : RDFExtractor.EXTRACTORS) {
				JSONObject resultDataEx = new JSONObject();
				resultDataEx.put("extractor", ex);
				resultDataEx.put("triples", triplesPerFormat.get(ex));
				resultDataEx.put("urls", urlsPerFormat.get(ex));
				resultDataEx.put("domains", sldsPerFormat.get(ex));
				resultDataEx.put("avgTriplesPerUrl", avgTriplesPerUrlPerFormat
						.get(ex).getAverage());
				resultDataEx.put("avgTriplesPerDomain",
						avgTriplesPerSldPerFormat.get(ex).getAverage());
				resultDataEx.put("topDomains", topDomainsPerFormat.get(ex)
						.toJsonArray());
				resultDataEx.put("topDomainsUrls",
						topDomainsPerFormatUrls.get(ex).toJsonArray());
				resultData.append("extractors", resultDataEx);
			}
			resultData.put("totalTriples", totalTriples);
			resultData.put("totalSizeBytes", totalBytes);
			resultData.put("minTimestamp", minDate);
			resultData.put("maxTimestamp", maxDate);

			resultData.put("totalSize",
					CSVExport.humanReadableByteCount(totalBytes, false));
			resultData.put("topDomains", topDomains.toJsonArray());
			resultData.put("topDomainsUrls", topDomainsUrls.toJsonArray());

		} catch (JSONException e) {
			log.warn("Failed to generate JSON", e);
		}
		return resultData;

	}

	private static String implode(List<String> list, String separator) {
		String ret = "";
		Iterator<String> it = list.iterator();
		while (it.hasNext()) {
			ret += it.next();
			if (it.hasNext()) {
				ret += separator;
			}
		}
		return ret;
	}

	private static JSONObject getObject(String extractorName, JSONArray arr)
			throws JSONException {
		for (int i = 0; i < arr.length(); i++) {
			JSONObject exData = arr.getJSONObject(i);

			String ex = exData.getString("extractor");
			if (ex.equals(extractorName)) {
				return exData;
			}
		}
		log.warn("Unable to find data for extractor " + extractorName);
		return null;
	}

	private static String htmlHeader = "<!DOCTYPE html>\n<html><head><title>Web Data Commons Extraction Report</title><link rel='stylesheet' href='http://webdatacommons.org/style.css' type='text/css' media='screen'/></head><body><a name='top'></a>";

	public static String generateHtml(JSONObject data, File outDir)
			throws IOException {
		String html = "";
		try {
			html += htmlHeader
					+ "<h1>Common Crawl Structured Data Extraction Results</h1>\n";
			html += "<p>This file contains the extraction report for the <a href=\"http://webdatacommons.org\">Web Data Commons</a> project. Both extraction and data statistics are given overall as well as for each structured data format.</p>\n";
			html += "<p>Apart from the obvious figures, we have counted every resource annnotated with a type as a \"Typed Entitiy\". \"Property values\" for these entities were also recorded, with the total figure split into \"URL values\", URLs that point to a different domain as \"Remote URL Values\", and also \"Literal Values\".</p>";

			JSONObject inputData = data.getJSONObject("data");
			JSONObject pageData = data.getJSONObject("pages");
			// JSONObject deepData = data.getJSONObject("deep");

			JSONArray extractorDataPages = pageData.getJSONArray("extractors");
			// JSONArray extractorDataDeep =
			// deepData.getJSONArray("extractors");

			// count
			long domainsTriples = 0;
			long typedEntities = 0;
			for (String ex : RDFExtractor.EXTRACTORS) {
				JSONObject exDataPages = getObject(ex, extractorDataPages);
				// JSONObject exDataDeep = getObject(ex, extractorDataDeep);
				domainsTriples += exDataPages.getLong("domains");
				// typedEntities += exDataDeep.getJSONObject("stats").getLong(
				// "totalEntities");
			}

			html += "<h2>Overall</h2><br /><table>";
			html += "<tr><th>Total Data</th><td>"
					+ inputData.getString("totalSize").replace("TiB",
							"Terabyte") + "</td><td>(compressed)</td></tr>";

			html += "<tr><th>Total URLs</th><td>"
					+ largeNumberFormat.format(inputData.getLong("pagesTotal"))
					+ "</td><td></td></tr>";
			html += "<tr><th>Parsed HTML URLs</th><td>"
					+ largeNumberFormat
							.format(inputData.getLong("pagesParsed"))
					+ "</td><td></td></tr>";

			html += "<tr><th>Domains with Triples</th><td>"
					+ largeNumberFormat.format(domainsTriples)
					+ "</td><td></td></tr>";

			html += "<tr><th>URLs with Triples</th><td>"
					+ largeNumberFormat.format(inputData
							.getLong("pagesWithTriples"))
					+ "</td><td></td></tr>";

			html += "<tr><th>Typed Entities</th><td>"
					+ largeNumberFormat.format(typedEntities)
					+ "</td><td></td></tr>";

			html += "<tr><th>Triples</th><td>"
					+ largeNumberFormat
							.format(pageData.getLong("totalTriples"))
					+ "</td><td></td></tr>";

			// JSONArray extractorData = pageData.getJSONArray("extractors");

			html += "</table>\n";

			html += "<h2>Results per Format</h2><br />";

			List<String> triples = new ArrayList<String>();
			List<String> urls = new ArrayList<String>();
			List<String> exl = new ArrayList<String>();
			List<String> domains = new ArrayList<String>();
			List<String> entities = new ArrayList<String>();

			html += "<table><tr><th>Extractor</th><th>Domains with Triples</th><th>URLs with Triples</th><th>Typed Entities</th><th>Triples</th></tr>";

			for (String ex : RDFExtractor.EXTRACTORS) {

				JSONObject exDataPages = getObject(ex, extractorDataPages);
				// JSONObject exDataDeep = getObject(ex, extractorDataDeep);

				exl.add(ex);

				html += "<tr><td><a href='#" + ex + "'>" + ex + "</a></td>";

				html += "<td>"
						+ largeNumberFormat.format(exDataPages
								.getLong("domains")) + "</td>";

				html += "<td>"
						+ largeNumberFormat.format(exDataPages.getLong("urls"))
						+ "</td>";

				// html += "<td>"
				// + Statistics.largeNumberFormat.format(exDataDeep
				// .getJSONObject("stats")
				// .getLong("totalEntities")) + "</td>";

				html += "<td>"
						+ largeNumberFormat.format(exDataPages
								.getLong("triples")) + "</td>";

				html += "</tr>";

				// stuff for graphs
				triples.add(exDataPages.getString("triples"));
				urls.add(exDataPages.getString("urls"));
				domains.add(exDataPages.getString("domains"));
				// entities.add(exDataDeep.getJSONObject("stats").getString(
				// "totalEntities"));

			}
			html += "</table>";

			html += "<img src=\"https://chart.googleapis.com/chart?chtt=Domains%20with%20Triples&chs=500x300&chds=a&cht=p&chd=t:"
					+ implode(domains, ",")
					+ "&chl="
					+ implode(exl, "|")
					+ "\" />";

			html += "<img src=\"https://chart.googleapis.com/chart?chtt=URLs%20with%20Triples&chs=500x300&chds=a&cht=p&chd=t:"
					+ implode(urls, ",")
					+ "&chl="
					+ implode(exl, "|")
					+ "\" />";

			html += "<br />";

			html += "<img src=\"https://chart.googleapis.com/chart?chtt=Typed%20Entities&chs=500x300&chds=a&cht=p&chd=t:"
					+ implode(entities, ",")
					+ "&chl="
					+ implode(exl, "|")
					+ "\" />";

			html += "<img src=\"https://chart.googleapis.com/chart?chtt=Triples&chs=500x300&chds=a&cht=p&chd=t:"
					+ implode(triples, ",")
					+ "&chl="
					+ implode(exl, "|")
					+ "\" />";

			html += "<h2>Top Domains by Extracted Triples</h2><br />";

			html += domainListToHtml(pageData.getJSONArray("topDomains"), 20,
					true, "Triples", outDir, "Top Domains by Extracted Triples");

			html += "<h2>Top Domains by URLs with Triples</h2><br />";

			html += domainListToHtml(pageData.getJSONArray("topDomainsUrls"),
					20, true, "URLs", outDir,
					"Top Domains by URLs with Triples");

			for (String ex : RDFExtractor.EXTRACTORS) {

				JSONObject exDataPages = getObject(ex, extractorDataPages);
				// JSONObject exDataDeep = getObject(ex, extractorDataDeep);

				html += "<a name='" + ex + "'></a><h3>Extractor " + ex
						+ "</h3><br /><table>";
				html += "<tr><th>Triples Extracted</th><td>"
						+ largeNumberFormat.format(exDataPages
								.getLong("triples")) + "</td></tr>";
				html += "<tr><th>URLs with Triples</th><td>"
						+ largeNumberFormat.format(exDataPages.getLong("urls"))
						+ "</td></tr>";
				html += "<tr><th>Average Triples per URL</th><td>"
						+ twoDForm.format(exDataPages
								.getDouble("avgTriplesPerUrl")) + "</td></tr>";
				html += "<tr><th>Domains with Triples</th><td>"
						+ largeNumberFormat.format(exDataPages
								.getLong("domains")) + "</td></tr>";
				html += "<tr><th>Average Triples per Domain</th><td>"
						+ twoDForm.format(exDataPages
								.getDouble("avgTriplesPerDomain"))
						+ "</td></tr>";
				html += "<tr><th>Top Domains by Extracted Triples</th><td>";

				html += domainListToHtml(
						exDataPages.getJSONArray("topDomains"), 10, false,
						"Triples", outDir,
						"Top Domains by Extracted Triples for Extractor " + ex);

				html += "<tr><th>Top Domains by URLs with Triples</th><td>";

				html += domainListToHtml(
						exDataPages.getJSONArray("topDomainsUrls"), 10, false,
						"URLs", outDir,
						"Top Domains by URLs with Triples for Extractor " + ex);
				html += "</td></tr>";

				// deep

				// html += "<tr><th>Typed Entities</th><td>"
				// + Statistics.largeNumberFormat.format(exDataDeep
				// .getJSONObject("stats")
				// .getLong("totalEntities")) + "</td></tr>";
				//
				// html += "<tr><th>Property Values</th><td>"
				// + Statistics.largeNumberFormat.format(exDataDeep
				// .getJSONObject("stats").getLong(
				// "totalProperties")) + "</td></tr>";
				//
				// html += "<tr><th>URL Values</th><td>"
				// + Statistics.largeNumberFormat.format(exDataDeep
				// .getJSONObject("stats").getLong("objectLinks"))
				// + "</td></tr>";
				//
				// html += "<tr><th>Remote URL Values</th><td>"
				// + Statistics.largeNumberFormat.format(exDataDeep
				// .getJSONObject("stats").getLong(
				// "objectRemoteLinks")) + "</td></tr>";
				//
				// html += "<tr><th>Literal Values</th><td>"
				// + Statistics.largeNumberFormat.format(exDataDeep
				// .getJSONObject("stats").getLong(
				// "objectLiterals")) + "</td></tr>";
				//
				// long typeProperties = exDataDeep.getJSONObject("stats")
				// .getLong("totalProperties")
				// - exDataDeep.getJSONObject("stats").getLong(
				// "objectLinks")
				// - exDataDeep.getJSONObject("stats").getLong(
				// "objectLiterals");
				//
				// html += "<tr><th>Other Values</th><td>"
				// + Statistics.largeNumberFormat.format(typeProperties)
				// + "</td></tr>";
				//
				// html += "<tr><th>Top Classes</th><td>"
				// + topkToHtml(exDataDeep.getJSONObject("stats")
				// .getJSONArray("topClasses"), 20, outDir,
				// "Top Classes for Extractor " + ex)
				// + "</td></tr>";
				//
				// html += "<tr><th>Top Properties</th><td>"
				// + topkToHtml(exDataDeep.getJSONObject("stats")
				// .getJSONArray("topProperties"), 20, outDir,
				// "Top Properties for Extractor " + ex)
				// + "</td></tr>";

				// end deep

				html += "</table>\n";
			}
			html += "</body></html>";
		} catch (JSONException e) {
			log.warn("Failed to generate HTML", e);
		}
		return html;
	}

	private static String tokToFile(JSONArray arr, File dir, String title)
			throws JSONException, IOException {
		String filename = title.toLowerCase().replace(" ", "_") + ".html";

		String html = htmlHeader
				+ "<h1>"
				+ title
				+ "</h1><br /><a href=\"stats.html\">Back to Statistics</a><br /><br />";

		html += "<ol>";
		for (int j = 0; j < arr.length(); j++) {
			JSONObject entry = arr.getJSONObject(j);
			html += "<li><abbr title='"
					+ entry.getString("key")
					+ "'>"
					+ Statistics.abbrv(entry.getString("key"))
					+ "</abbr> ("
					+ Statistics.largeNumberFormat.format(entry
							.getLong("count")) + " Entities)</li>";
		}
		html += "</ol><br />\n</body>";

		Writer w = new FileWriter(new File(dir + File.separator + filename));
		w.write(html);
		w.close();

		return filename;
	}

	private static String topkToHtml(JSONArray arr, int limit, File dir,
			String title) throws JSONException, IOException {
		String html = "";
		String id = UUID.randomUUID().toString();

		html += "<small><a href='' onclick=\"document.getElementById('"
				+ id
				+ "').style.display='block';this.style.display='none';return false;\">Show top values</a></small><ol id='"
				+ id + "' style='display:none'>";
		for (int j = 0; j < Math.min(limit, arr.length()); j++) {
			JSONObject entry = arr.getJSONObject(j);
			html += "<li><abbr title='"
					+ entry.getString("key")
					+ "'>"
					+ Statistics.abbrv(entry.getString("key"))
					+ "</abbr> ("
					+ Statistics.largeNumberFormat.format(entry
							.getLong("count")) + " Entities)</li>";
		}
		html += "";
		if (arr.length() > limit) {
			String htmlFile = tokToFile(arr, dir, title);
			html += "<li><a href=\"" + htmlFile + "\">More</a></li></ol>";
		} else {
			html += "</ol>";
		}
		html += "<br /> \n";

		return html;
	}

	public static String domainListToHtml(JSONArray arr, int limit,
			boolean expanded, String unit, File dir, String title)
			throws JSONException, IOException {
		if (arr.length() == 0) {
			return "";
		}
		String id = UUID.randomUUID().toString();
		String html = "";
		if (!expanded) {
			html += "<small><a href='' onclick=\"document.getElementById('"
					+ id
					+ "').style.display='block';this.style.display='none';return false;\">Show top domains</a></small><ol id='"
					+ id + "' style='display:none'>";
		} else {
			html += "<ol>";
		}
		for (int j = 0; j < Math.min(limit, arr.length()); j++) {
			JSONObject domainData = arr.getJSONObject(j);
			html += "<li><a href='http://www." + domainData.getString("key")
					+ "'>" + domainData.getString("key") + "</a> ("
					+ largeNumberFormat.format(domainData.getLong("count"))
					+ " " + unit + ")</li>";
		}
		if (arr.length() > limit) {
			String htmlFile = domainListToFile(arr, unit, dir, title);
			html += "<li><a href=\"" + htmlFile + "\">More</a></li></ol>";
		} else {
			html += "</ol>";
		}
		html += "<br /> \n";

		return html;
	}

	public static String domainListToFile(JSONArray arr, String unit, File dir,
			String title) throws JSONException, IOException {

		String filename = title.toLowerCase().replace(" ", "_") + ".html";

		String html = htmlHeader
				+ "<h1>"
				+ title
				+ "</h1><br /><a href=\"stats.html\">Back to Statistics</a><br /><br />";

		html += "<ol>";
		for (int j = 0; j < arr.length(); j++) {
			JSONObject domainData = arr.getJSONObject(j);
			html += "<li><a href='http://www." + domainData.getString("key")
					+ "'>" + domainData.getString("key") + "</a> ("
					+ largeNumberFormat.format(domainData.getLong("count"))
					+ " " + unit + ")</li>";
		}

		html += "</ol></body>";

		Writer w = new FileWriter(new File(dir + File.separator + filename));
		w.write(html);
		w.close();

		return filename;
	}

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
		@SuppressWarnings("unused")
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
				String smallestKey = "";
				long smallestValue = Long.MAX_VALUE;
				for (Map.Entry<String, Long> entry : entries.entrySet()) {
					if (entry.getValue() < smallestValue) {
						smallestKey = entry.getKey();
						smallestValue = entry.getValue();
					}
				}
				entries.remove(smallestKey);
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

	/*
	 * Durchschnittl. Anzahl Tripel auf einer URL Gesamtschnitt Durchschnittl.
	 * Anzahl Tripel auf einer Domain Gesamtschnitt
	 */

	public static class MapUtil {
		public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue(
				Map<K, V> map) {
			List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>(
					map.entrySet());
			Collections.sort(list, new Comparator<Map.Entry<K, V>>() {
				public int compare(Map.Entry<K, V> o1, Map.Entry<K, V> o2) {
					return (o1.getValue()).compareTo(o2.getValue()) * -1;
				}
			});

			Map<K, V> result = new LinkedHashMap<K, V>();
			for (Map.Entry<K, V> entry : list) {
				result.put(entry.getKey(), entry.getValue());
			}
			return result;
		}
	}

	private static void add(Map<String, Long> map, String key, Long value) {
		if (value == null || value < 1) {
			return;
		}
		if (!map.containsKey(key)) {
			map.put(key, value);
		} else {
			map.put(key, map.get(key) + value);
		}
	}

	private static final Pattern DOMAIN_PATTERN = Pattern
			.compile("http(s)?://(([a-zA-Z0-9-]+(\\.)?)+)");

	public static String getDomain(String uri) {
		try {
			Matcher m = DOMAIN_PATTERN.matcher(uri);
			if (m.find()) {
				return m.group(2);
			}
		} catch (Exception e) {
			log.warn(uri, e);
		}
		return uri;
	}

	/**
	 * Main method to run the Statistics generator from the command line
	 * */
	public static void main(String[] args) throws IOException, JSAPException {
		JSAP jsap = new JSAP();
		FlaggedOption inStatDir = new FlaggedOption("statdir")
				.setStringParser(JSAP.STRING_PARSER).setRequired(true)
				.setLongFlag("statdir").setShortFlag('i');
		inStatDir.setHelp("Extraction statistics directory");
		jsap.registerParameter(inStatDir);

		FlaggedOption outDir = new FlaggedOption("outdir")
				.setStringParser(JSAP.STRING_PARSER).setRequired(true)
				.setLongFlag("outdir").setShortFlag('o');
		outDir.setHelp("Output directory");
		jsap.registerParameter(outDir);

		JSAPResult config = jsap.parse(args);

		if (!config.success()) {
			System.err.println("Usage: " + Statistics.class.getName() + " "
					+ jsap.getUsage());
			System.err.println(jsap.getHelp());
			System.exit(1);
		}

		Reader dataReader = getReader(getFile(config.getString("statdir"),
				"data.csv"));

		Reader pageReader = getReader(getFile(config.getString("statdir"),
				"pages.csv"));

		Writer htmlWriter = new FileWriter(new File(config.getString("outdir")
				+ File.separator + "stats.html"));

		File jsonFile = new File(config.getString("outdir") + File.separator
				+ "stats.json");

		// File dataStatsFile = new File(config.getString("outdir")
		// + File.separator + "datastats.json");

		if (!jsonFile.exists()) {
			Writer jsonWriter = new FileWriter(jsonFile);
			createStat(dataReader, pageReader, jsonWriter);
		}
		try {
			JSONObject statsObject = new JSONObject(new JSONTokener(
					new FileReader(jsonFile)));
			// JSONObject dataObject = new JSONObject(new JSONTokener(
			// new FileReader(dataStatsFile)));
			// statsObject.put("deep", dataObject);

			htmlWriter.write(generateHtml(statsObject,
					new File(config.getString("outdir"))));
			htmlWriter.close();
		} catch (JSONException e) {
			log.warn("Failed to read " + jsonFile);
			e.printStackTrace();
		}

		log.info("Wrote results to " + config.getString("outdir"));
	}

	public static void createStat(Reader dataStatReader, Reader pageStatReader,
			Writer jsonWriter) {
		JSONObject overallJson = new JSONObject();
		try {
			JSONObject dataStat = readDataStat(dataStatReader);
			overallJson.put("data", dataStat);

			JSONObject pageStat = readPageStat(pageStatReader);
			overallJson.put("pages", pageStat);
			overallJson.write(jsonWriter);
			jsonWriter.close();

		} catch (Exception e) {
			log.warn(e);
		}
	}

	private static final File getFile(String directory, String prefix)
			throws FileNotFoundException, IOException {
		File dir = new File(directory);
		String[] files = dir.list();
		if (files == null) {
			log.warn("Unable to find test files in directory " + dir);
		}

		for (int i = 0; i < files.length; i++) {
			if (files[i].startsWith(prefix)) {
				return new File(dir.toString() + File.separator + files[i]);
			}
		}
		throw new FileNotFoundException();
	}

	public static Reader getReader(File f) throws FileNotFoundException,
			IOException {
		if (f.getName().endsWith(".gz")) {
			return new InputStreamReader(new GZIPInputStream(
					new FileInputStream(f)));
		} else {
			return new FileReader(f);
		}
	}

	private static final Prefixes prefixes = new Prefixes();
	static {

		// load popular prefixes from prefix.cc
		Properties p = new Properties();
		try {
			p.load(TripleStats.class.getClassLoader().getResourceAsStream(
					"prefixes.ini"));
			for (Map.Entry<Object, Object> e : p.entrySet()) {
				prefixes.add((String) e.getKey(), (String) e.getValue());
			}
		} catch (IOException e) {
			log.warn("Unable to load prefixes ini");
		}

		// manually set some missing prefixes here
		prefixes.add("myspace2",
				"http://x.myspacecdn.com/modules/sitesearch/static/rdf/profileschema.rdf#");
		prefixes.add("datavoc", "http://data-vocabulary.org/");
		prefixes.add("icaltzd", "http://www.w3.org/2002/12/cal/icaltzd#");
		prefixes.add("datavoc-w", "http://www.data-vocabulary.org/");
		prefixes.add("searchmonkey", "http://search.yahoo.com/searchmonkey/");
		prefixes.add("media-purl", "http://purl.org/media#");
		prefixes.add("purl-stuff-rev", "http://www.purl.org/stuff/rev");
		prefixes.add("freebase-id", "http://www.freebase.com/id/");
		prefixes.add("schema-org", "http://www.schema.org/");

	}

	public static String abbrv(String url) {
		if (prefixes.canAbbreviate(url)) {
			return prefixes.abbreviate(url);
		}
		log.warn("Cant abbreviate " + url);
		return url;
	}
}
