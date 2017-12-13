package org.webdatacommons.structureddata.tablegen;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPInputStream;

import org.apache.log4j.Logger;
import org.webdatacommons.structureddata.extractor.RDFExtractor;
import org.webdatacommons.structureddata.tablegen.StreamingStatsGenerator.PropertyAndDomainAggregationEntry;
import org.webdatacommons.structureddata.util.Statistics;

import com.amazonaws.util.json.JSONArray;
import com.amazonaws.util.json.JSONException;
import com.amazonaws.util.json.JSONObject;
import com.amazonaws.util.json.JSONTokener;
import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;

public class TripleStats {
	private static Logger log = Logger.getLogger(TripleStats.class);

	private static final int CACHE_WINDOW_SIZE = 500;

	public static JSONObject generateStats(Reader inputRdf, String extractor)
			throws JSONException {
		log.info("Reading data for " + extractor);
		StreamingStatsGenerator rc = new StreamingStatsGenerator(
				CACHE_WINDOW_SIZE);
		JSONObject jo = new JSONObject();

		try {
			rc.addNquads(inputRdf);
		} catch (Exception e) {
			log.warn(e);
		}

		rc.finish();
		log.info("Generating Statistics for " + extractor);

		/*
		 * jo.put("propertyCoocurrence", itemsetsToJson(
		 * rc.getPropertiesByDomain().getCoocSorted( rc.getDomainsRead(), 0.1,
		 * 4), rc.getEntitiesRead()));
		 */

		jo.put("topClasses", rc.getTopClasses().toJsonArray());
		jo.put("topProperties", rc.getTopProperties().toJsonArray());

		jo.put("totalDomains", rc.getDomainsRead());
		jo.put("totalEntities", rc.getEntitiesRead());
		jo.put("totalProperties", rc.getTotalProperties());

		jo.put("objectLinks", rc.getObjectLinks());
		jo.put("objectRemoteLinks", rc.getObjectRemoteLinks());
		jo.put("objectLiterals", rc.getObjectLiterals());

		log.info("Analyzed " + rc.getEntitiesRead() + " entities from "
				+ rc.getDomainsRead() + " domains.for " + extractor);

		return jo;
	}

	@SuppressWarnings("unused")
	private static JSONArray itemsetsToJson(
			List<Map.Entry<Set<String>, PropertyAndDomainAggregationEntry>> entries,
			long transactions) throws JSONException {
		JSONArray itemsets = new JSONArray();

		for (Map.Entry<Set<String>, PropertyAndDomainAggregationEntry> e : entries) {
			double percentage = (e.getValue().getSupport() * 1.0 / transactions) * 100;

			JSONObject group = new JSONObject();
			JSONArray properties = new JSONArray();
			for (String p : e.getKey()) {
				JSONObject property = new JSONObject();
				property.put("uri", p);
				property.put("abbrv", Statistics.abbrv(p));
				properties.put(property);
			}
			group.put("properties", properties);
			group.put("entities", e.getValue().getTotalEntities());
			group.put("percentage", percentage);
			group.put("domains", e.getValue().getTopDomains().toJsonArray());
			itemsets.put(group);
		}
		return itemsets;
	}

	public static final String DATA_PREFIX = "ccrdf.";

	public static class LoggingFileInputStream extends FileInputStream {
		File f;
		// long bytesRead = 0;
		private static Map<File, Boolean> printedFiles = new HashMap<File, Boolean>();

		public LoggingFileInputStream(File file) throws FileNotFoundException {
			super(file);
			f = file;
		}

		public int read(byte[] b, int off, int len) throws IOException {
			if (!printedFiles.containsKey(f) && len > 10) {
				log.info("Reading from " + f);
				printedFiles.put(f, true);
			}
			int read = super.read(b, off, len);
			// bytesRead += read;

			// log.info("Read " + bytesRead + " bytes from " + f + "(" + len +
			// ")");

			return read;
		}
	}

	public static JSONObject createFullStats(final File dataDir, File resultDir)
			throws JSONException {
		final JSONArray jo = new JSONArray();

		ExecutorService ex = Executors.newCachedThreadPool();
		for (final String currentExtractor : RDFExtractor.EXTRACTORS) {

			final File partialCacheFile = new File(resultDir + File.separator
					+ currentExtractor + ".part.json");
			if (partialCacheFile.exists()) {
				try {
					JSONObject statsObject = new JSONObject(new JSONTokener(
							new FileReader(partialCacheFile)));
					jo.put(statsObject);
					log.info("Read results for " + currentExtractor + " from "
							+ partialCacheFile);
					continue;
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
			}

			ex.submit(new Thread() {
				public void run() {
					List<InputStream> fileStreams = new ArrayList<InputStream>();
					int i = 0;
					File dataFile;
					while (true) {
						dataFile = new File(dataDir + File.separator
								+ DATA_PREFIX + currentExtractor + "." + i
								+ ".nq.gz");
						if (!dataFile.exists()) {
							break;
						}
						try {
							fileStreams.add(new GZIPInputStream(
									new LoggingFileInputStream(dataFile)));
						} catch (Exception e1) {
							log.warn("I/O failed on " + dataFile, e1);
						}
						i++;
					}
					log.debug(currentExtractor + ": " + fileStreams.size()
							+ " files");

					Reader combinedStream = new InputStreamReader(
							new SequenceInputStream(Collections
									.enumeration(fileStreams)));

					JSONObject extractorO = new JSONObject();
					try {

						extractorO.put("extractor", currentExtractor);
						extractorO
								.put("stats",
										generateStats(combinedStream,
												currentExtractor));
						FileWriter partWriter = new FileWriter(partialCacheFile);
						extractorO.write(partWriter);
						partWriter.close();

					} catch (JSONException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}

					jo.put(extractorO);
				}
			});
		}
		JSONObject result = new JSONObject();
		ex.shutdown();
		try {
			ex.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
			result.put("extractors", jo);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		return result;
	}

	public static String generateHtml(JSONObject data) throws JSONException {
		String html = "";
		html += "<!DOCTYPE html>\n<html><head><title>Common Crawl Structured Triple Analysis Results</title><link rel='stylesheet' href='http://www4.wiwiss.fu-berlin.de/bizer/sieve/style.css' type='text/css' media='screen'/></head><body><a name='top'></a><h1>Common Crawl Structured Data Triple Analysis Results</h1>";
		html += "<h2>Results per Format</h2><br />";

		JSONArray extractorData = data.getJSONArray("extractors");

		for (int i = 0; i < extractorData.length(); i++) {
			String ex = extractorData.getJSONObject(i).getString("extractor");
			html += "<a href='#" + ex + "'>" + ex + "</a> ";
		}

		for (int i = 0; i < extractorData.length(); i++) {
			JSONObject exData = extractorData.getJSONObject(i);
			String extractorName = exData.getString("extractor");
			html += "<a name='" + extractorName + "'></a><h3>Extractor "
					+ extractorName + "</h3><br /><table>";

			/*
			 * html += "<tr><th>Total Domains</th><td>" +
			 * Statistics.largeNumberFormat.format(exData.getJSONObject(
			 * "stats").getLong("totalDomains")) + "</td></tr>";
			 */
			html += "<tr><th>Total Entities</th><td>"
					+ Statistics.largeNumberFormat.format(exData.getJSONObject(
							"stats").getLong("totalEntities")) + "</td></tr>";

			html += "<tr><th>Total Properties</th><td>"
					+ Statistics.largeNumberFormat.format(exData.getJSONObject(
							"stats").getLong("totalProperties")) + "</td></tr>";

			html += "<tr><th>URL Values</th><td>"
					+ Statistics.largeNumberFormat.format(exData.getJSONObject(
							"stats").getLong("objectLinks")) + "</td></tr>";

			html += "<tr><th>Remote URL Values</th><td>"
					+ Statistics.largeNumberFormat.format(exData.getJSONObject(
							"stats").getLong("objectRemoteLinks"))
					+ "</td></tr>";

			html += "<tr><th>Literal Values</th><td>"
					+ Statistics.largeNumberFormat.format(exData.getJSONObject(
							"stats").getLong("objectLiterals")) + "</td></tr>";

			html += "</table>";

			/*
			 * html += "<h4>Properties used together</h4><br />" +
			 * itemsetsToHtml(exData.getJSONObject("stats")
			 * .getJSONArray("propertyCoocurrence"));
			 */
			html += "<h4>Top Properties</h4><br />"
					+ topkToHtml(
							exData.getJSONObject("stats").getJSONArray(
									"topProperties"), 20);
			html += "<h4>Top Classes</h4><br />"
					+ topkToHtml(
							exData.getJSONObject("stats").getJSONArray(
									"topClasses"), 20);

		}

		html += "</body></html>";
		return html;
	}

	/*private static String itemsetsToHtml(JSONArray arr) throws JSONException {
		String html = "<table>";
		for (int j = 0; j < arr.length(); j++) {
			JSONObject entry = arr.getJSONObject(j);

			html += "<tr><td>";
			JSONArray properties = entry.getJSONArray("properties");
			for (int i = 0; i < properties.length(); i++) {
				html += "<abbr title='"
						+ properties.getJSONObject(i).getString("uri") + "'>"
						+ properties.getJSONObject(i).getString("abbrv")
						+ "</abbr> ";
			}

			html += "</td><td>"
					+ Statistics.twoDForm.format(entry.getDouble("percentage"))
					+ "% ("
					+ Statistics.largeNumberFormat.format(entry
							.getLong("entities")) + ")</td>";

			html += "<td>"
					+ Statistics.domainListToHtml(
							entry.getJSONArray("domains"), 10, false,
							"Entities") + "</td></tr>";
		}
		html += "</table><br />";

		return html;
	}*/

	private static String topkToHtml(JSONArray arr, int limit)
			throws JSONException {
		String html = "<ol>";
		for (int j = 0; j < Math.min(limit, arr.length()); j++) {
			JSONObject entry = arr.getJSONObject(j);
			html += "<li><abbr title='"
					+ entry.getString("key")
					+ "'>"
					+ Statistics.abbrv(entry.getString("key"))
					+ "</abbr> ("
					+ Statistics.largeNumberFormat.format(entry
							.getLong("count")) + ")</li>";
		}
		html += "</ol><small>(More in <a href='datastats.json'>JSON</a>)</small><br />";

		return html;
	}

	public static void main(String[] args) throws IOException, JSAPException {
		JSAP jsap = new JSAP();
		FlaggedOption inDirO = new FlaggedOption("datadir")
				.setStringParser(JSAP.STRING_PARSER).setRequired(true)
				.setLongFlag("datadir").setShortFlag('d');
		inDirO.setHelp("Directory with RDF files from CC export");
		jsap.registerParameter(inDirO);

		FlaggedOption outDirO = new FlaggedOption("outdir")
				.setStringParser(JSAP.STRING_PARSER).setRequired(true)
				.setLongFlag("outdir").setShortFlag('o');
		outDirO.setHelp("Directory to write statistics to");
		jsap.registerParameter(outDirO);

		JSAPResult config = jsap.parse(args);

		if (!config.success()) {
			System.err.println("Usage: " + TripleStats.class.getName() + " "
					+ jsap.getUsage());
			System.err.println(jsap.getHelp());
			System.exit(1);
		}

		File inDir = new File(config.getString("datadir"));
		File outDir = new File(config.getString("outdir"));
		if (!outDir.exists()) {
			outDir.mkdirs();
		}

		try {
			JSONObject dataStats = createFullStats(inDir, outDir);

			// Write JSON
			FileWriter fwj = new FileWriter(new File(outDir + File.separator
					+ "datastats.json"));
			dataStats.write(fwj);
			fwj.close();

			// Write HTML
			FileWriter fwh = new FileWriter(new File(outDir + File.separator
					+ "datastats.html"));
			fwh.write(generateHtml(dataStats));
			fwh.close();

		} catch (JSONException e) {
			e.printStackTrace();
		}

	}

}
