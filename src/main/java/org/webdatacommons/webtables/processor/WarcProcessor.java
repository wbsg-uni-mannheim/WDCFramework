package org.webdatacommons.webtables.processor;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Constructor;
import java.net.UnknownHostException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.IllegalCharsetNameException;
import java.nio.charset.UnsupportedCharsetException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.model.S3Object;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jwat.warc.WarcReader;
import org.jwat.warc.WarcReaderFactory;
import org.jwat.warc.WarcRecord;
import org.webdatacommons.framework.processor.FileProcessor;
import org.webdatacommons.framework.processor.ProcessingNode;
import org.webdatacommons.webtables.extraction.ExtractionAlgorithm;
import org.webdatacommons.webtables.extraction.TableClassification;
import org.webdatacommons.webtables.extraction.model.DocumentMetadata;
import org.webdatacommons.webtables.extraction.stats.HashMapStatsData;
import org.webdatacommons.webtables.extraction.stats.StatsKeeper;
import org.webdatacommons.webtables.tools.data.Dataset;

import de.dwslab.dwslib.util.io.OutputUtil;
/**
 * 
 * Processor to extract web tables from .warc files.
 * The code was mainly copied from the DWTC framework 
 * (https://github.com/JulianEberius/dwtc-extractor & https://github.com/JulianEberius/dwtc-tools)
 * 
 * @author Robert Meusel (robert@informatik.uni-mannheim.de) - Translation to DPEF
 *
 */
public class WarcProcessor extends ProcessingNode implements FileProcessor {

	private static Logger log = Logger.getLogger(WarcProcessor.class);

	private static final String WARC_TARGET_URI = "WARC-Target-URI";

	@Override
	public Map<String, String> process(ReadableByteChannel fileChannel,
			String inputFileKey) throws Exception {

		log.info("Extracting data from " + inputFileKey + " ...");
		WarcReader warcReader = WarcReaderFactory.getReaderCompressed(Channels
				.newInputStream(fileChannel));

		long pagesTotal = 0;
		long pagesErrors = 0;
		long start = System.currentTimeMillis();

		// stats
		StatsKeeper stats = new HashMapStatsData();
		// extrac terms
		boolean extractTopNTerms = Boolean
				.parseBoolean(getOrCry("extractTopNTerms"));
		// table classifier
		// define the model for classification - phase 1 and phase 2
		TableClassification tc = new TableClassification(
				getOrCry("phase1ModelPath"), getOrCry("phase2ModelPath"));

		@SuppressWarnings("rawtypes")
		Constructor c = Class.forName(getOrCry("extractionAlgorithm"))
				.getConstructor(StatsKeeper.class, Boolean.TYPE,
						TableClassification.class);
		ExtractionAlgorithm ea = (ExtractionAlgorithm) c.newInstance(stats,
				extractTopNTerms, tc);

		// read all entries in the ARC file
		RecordWithOffsetsAndURL item;
		item = getNextResponseRecord(warcReader);
		List<Dataset> result = new ArrayList<Dataset>();
		while (item != null) {
			List<Dataset> docResult;
			try {
				Document doc;
				DocumentMetadata dm = new DocumentMetadata(item.start,
						item.end, inputFileKey, item.url, item.lastModified);

				try {
					// try parsing with charset detected from doc
					doc = Jsoup.parse(new ByteArrayInputStream(item.bytes),
							null, "");
					docResult = ea.extract(doc, dm);
				} catch (IllegalCharsetNameException
						| UnsupportedCharsetException e) {
					try {
						// didnt work, try parsing with utf-8 as
						// charset
						doc = Jsoup.parse(new ByteArrayInputStream(item.bytes),
								"UTF-8", "");
						docResult = ea.extract(doc, dm);
					} catch (IllegalCharsetNameException
							| UnsupportedCharsetException e2) {
						// didnt work either, no result
						docResult = new ArrayList<Dataset>();
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
				docResult = new ArrayList<>();
			}
			// add per-doc result to per-warc-file result
			if (docResult != null)
				result.addAll(docResult);
			pagesTotal++;
			// next record with one retry
			item = getNextResponseRecord(warcReader);
		}
		warcReader.close();

		// upload result to S3
		upload(inputFileKey, result);

		double duration = (System.currentTimeMillis() - start) / 1000.0;
		double rate = (pagesTotal * 1.0) / duration;

		// create data file statistics
		Map<String, String> dataStats = new HashMap<String, String>();
		for (Map.Entry<String, Integer> entry : ea.getStatsKeeper()
				.statsAsMap().entrySet()) {
			dataStats.put(entry.getKey(), String.valueOf(entry.getValue()));
		}
		dataStats.put("duration", Double.toString(duration));
		dataStats.put("rate", Double.toString(rate));
		dataStats.put("pagesTotal", Long.toString(pagesTotal));
		dataStats.put("pagesErrors", Long.toString(pagesErrors));

		log.info("Extracted data from " + inputFileKey + " - parsed "
				+ pagesTotal + " pages in " + duration + " seconds, " + rate
				+ " pages/sec");

		return dataStats;
	}

	private static class RecordWithOffsetsAndURL {
		public byte[] bytes;
		public long start;
		public long end;
		public String url;
		public String lastModified;

		public RecordWithOffsetsAndURL(byte[] bytes, long start, long end,
				String url, String lastModified) {
			super();
			this.bytes = bytes;
			this.start = start;
			this.end = end;
			this.url = url;
			this.lastModified = lastModified;
		}
	}

	private RecordWithOffsetsAndURL getNextResponseRecord(WarcReader warcReader)
			throws IOException {
		WarcRecord wr;
		while (true) {
			try {
				wr = warcReader.getNextRecord();
			} catch (IOException e) {
				continue;
			}
			if (wr == null)
				return null;

			long offset = warcReader.getStartOffset();
			String type = wr.getHeader("WARC-Type").value;
			if (type.equals("response")) {
				byte[] rawContent = IOUtils.toByteArray(wr.getPayloadContent());
				long endOffset = warcReader.getOffset();
				String url = wr.getHeader(WARC_TARGET_URI).value;
				String lastModified = null;
				if (wr.getHttpHeader().getHeader("Last-Modified") != null)
					lastModified = wr.getHttpHeader()
							.getHeader("Last-Modified").value;
				return new RecordWithOffsetsAndURL(rawContent, offset,
						endOffset, url, lastModified);
			}
		}
	}

	private String makeOutputFileKey(String inputFileKey) {
		int idx = inputFileKey.indexOf(".warc");
		String s = inputFileKey.substring(0, idx) + ".json.gz";
		return s;
	}

	private void upload(String inputFileKey, Iterable<Dataset> results)
			throws IOException, UnknownHostException, S3ServiceException,
			NoSuchAlgorithmException {

		File tmpFile = File.createTempFile(inputFileKey, "");
		FileOutputStream output = new FileOutputStream(tmpFile);
		Writer writer = null;
		try {
			writer = OutputUtil.getGZIPBufferedWriter(tmpFile);
			for (Dataset res : results) {
				writer.append(res.toJson());
				writer.append("\n");
			}
		} finally {
			if (writer != null)
				writer.close();
			output.close();
		}
		if (tmpFile.exists()) {

			S3Object dataFileObject = new S3Object(tmpFile);
			dataFileObject.setKey(makeOutputFileKey(inputFileKey));
			getStorage().putObject(getOrCry("resultBucket"), dataFileObject);
			tmpFile.delete();
		}
	}

}
