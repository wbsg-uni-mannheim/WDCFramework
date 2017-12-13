package org.webdatacommons.structureddata.processor;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.httpclient.HeaderElement;
import org.apache.log4j.Logger;
import org.commoncrawl.protocol.shared.ArcFileHeaderItem;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.commoncrawl.util.shared.FlexBuffer;
import org.webdatacommons.framework.io.LoggingStatHandler;
import org.webdatacommons.framework.io.StatHandler;
import org.webdatacommons.structureddata.extractor.RDFExtractor;
import org.webdatacommons.structureddata.extractor.RDFExtractor.ExtractorResult;

public class UltraFailArcProcessor extends ArcProcessor {
	private static Logger log = Logger.getLogger(UltraFailArcProcessor.class);

	enum State {
		inArcHeader, inEntryHeader, inHttpHeader, inContent
	}

	private static final int arcHeaderLines = 4;

	private State currentState = State.inArcHeader;

	private static Pattern arcHeaderPattern = Pattern
			.compile("(.*) (\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}) (\\d+) ([^ ]+) (\\d+)");

	static class EntryHeader {
		String url;
		String ip;
		long date;
		String contentType;
		int length;
	}

	private static EntryHeader parseArcHeader(String arcHeaderStr) {
		Matcher m = arcHeaderPattern.matcher(arcHeaderStr);
		if (m.matches()) {
			try {
				EntryHeader h = new EntryHeader();
				h.url = m.group(1);
				h.ip = m.group(2);
				h.date = Long.parseLong(m.group(3));
				h.contentType = m.group(4);
				h.length = Integer.parseInt(m.group(5));
				return h;
			} catch (Exception e) {
				throw new IllegalArgumentException("Unable to parse "
						+ arcHeaderStr, e);
			}
		}
		throw new IllegalArgumentException("Unable to parse " + arcHeaderStr);
	}

	private static class CountingReader {

		private Reader r;
		private int bytesRead = 0;
		private boolean done = false;

		public CountingReader(Reader r) {
			this.r = r;
		}

		public String readLine() {
			if (done) {
				return null;
			}
			try {
				StringBuffer b = new StringBuffer();
				while (true) {
					int i = r.read();
					char c = (char) i;
					bytesRead++;
					done = (i == -1);
					if (done || c == '\n') {
						return b.toString();
					}
					b.append(c);
				}

			} catch (IOException e) {
				log.warn(e);
				return null;
			}
		}

		public int getReadBytes() {
			return bytesRead;
		}

		public void resetReadBytes() {
			bytesRead = 0;
		}
	}

	public Map<String, String> processUnpackedArcData(final Reader arcFile,
			String arcFileName, RDFExtractor extractor, StatHandler statHandler)
			throws IOException {

		// ArcProcessor Stuff
		long pagesTotal = 0;
		long pagesParsed = 0;
		long pagesErrors = 0;

		long pagesTriples = 0;

		long start = System.currentTimeMillis();

		// Reader Stuff
		CountingReader br = new CountingReader(arcFile);

		int arcHeaderLine = 0;
		EntryHeader entryHeader = null;
		int headerLines = 0;
		ArcFileItem currentItem = null;
		StringBuffer currentContent = new StringBuffer();

		ArrayList<ArcFileHeaderItem> currentHeaders = new ArrayList<ArcFileHeaderItem>();
		boolean parseHeaders = false;

		String currentLine = "";
		boolean advanceLine = true;

		
		while (true) {
			if (advanceLine) {
				currentLine = br.readLine();
				if (currentLine == null) {
					break;
				}
			}
			advanceLine = false;

			switch (currentState) {
			// skip the first lines
			case inArcHeader:
				arcHeaderLine++;
				if (arcHeaderLine >= arcHeaderLines) {
					currentState = State.inEntryHeader;
				}
				advanceLine = true;
				break;

			case inEntryHeader:
				// skip if empty
				if (currentLine.trim().equals("")) {
					advanceLine = true;
					break;
				}
				entryHeader = parseArcHeader(currentLine);

				currentItem = new ArcFileItem();

				currentItem.setArcFileName(arcFileName);
				currentItem.setHostIP(entryHeader.ip);
				currentItem.setMimeType(entryHeader.contentType);
				currentItem.setRecordLength(entryHeader.length);
				currentItem.setTimestamp(entryHeader.date);
				currentItem.setUri(entryHeader.url);

				currentState = State.inHttpHeader;
				br.resetReadBytes();

				advanceLine = true;
				break;

			case inHttpHeader:
				if (br.getReadBytes() >= entryHeader.length) {
					currentState = State.inEntryHeader;
					log.debug("Skipped " + entryHeader.url + " - no content");
					advanceLine = true;
					break;
				}
				if (currentLine.trim().startsWith("<")
						|| (headerLines > 0 && !currentLine.contains(":"))) {
					currentItem.setHeaderItems(currentHeaders);
					currentHeaders = new ArrayList<ArcFileHeaderItem>();
					currentState = State.inContent;
					advanceLine = false;
					headerLines = 0;
				} else {
					if (parseHeaders) {
						HeaderElement[] headerLine = HeaderElement
								.parseElements(currentLine);
						for (int i = 0; i < headerLine.length; i++) {
							HeaderElement he = headerLine[i];
							if (he == null || he.getName() == null
									|| he.getValue() == null) {
								continue;
							}
							ArcFileHeaderItem hi = new ArcFileHeaderItem();
							hi.setItemKey(he.getName());
							hi.setItemValue(he.getValue());
							currentHeaders.add(hi);
						}
					}
					headerLines++;
					advanceLine = true;
				}
				break;

			case inContent:
				currentContent.append(currentLine);
				currentContent.append("\n");
				advanceLine = true;

				if (br.getReadBytes() >= entryHeader.length) {
					currentItem.setContent(new FlexBuffer(currentContent
							.toString().getBytes(), false));

					currentContent = new StringBuffer();

					// Handle current item here
					pagesTotal++;

					if (extractor.supports(currentItem.getMimeType())) {
						// do extraction (woo ho)
						pagesParsed++;

						ExtractorResult result = extractor.extract(currentItem);

						// if we had an error, increment error count
						if (result.hadError()) {
							pagesErrors++;
						}
						// if we found no triples, continue
						if (result.hadResults()) {
							// collect some other statistics
							Map<String, String> stats = new HashMap<String, String>();
							stats.putAll(itemStats(currentItem));
							stats.putAll(result.getExtractorTriples());
							stats.putAll(result.getReferencedData());
							stats.put("detectedMimeType", result.getMimeType());
							stats.put("totalTriples",
									Long.toString(result.getTotalTriples()));

							// write statistics
							statHandler.addStats(currentItem.getUri(), stats);
							pagesTriples++;
						}
					}

					// Continue with parser stuff
					currentState = State.inEntryHeader;
				}
				break;
			}
		}

		statHandler.flush();

		double duration = (System.currentTimeMillis() - start) / 1000.0;
		double rate = (pagesTotal * 1.0) / duration;

		// create data file statistics and return
		Map<String, String> dataStats = new HashMap<String, String>();
		dataStats.put("duration", Double.toString(duration));
		dataStats.put("rate", Double.toString(rate));
		dataStats.put("pagesTotal", Long.toString(pagesTotal));
		dataStats.put("pagesParsed", Long.toString(pagesParsed));
		dataStats.put(PAGES_GUESSED_TRIPLES, Long.toString(pagesTriples));
		dataStats.put("pagesErrors", Long.toString(pagesErrors));

		log.info("Extracted data from " + arcFileName + " - parsed "
				+ pagesTotal + " pages in " + duration + " seconds, " + rate
				+ " pages/sec");

		return dataStats;

	}

	@SuppressWarnings("resource")
	public static void main(String[] args) throws FileNotFoundException,
			IOException {
		String filename = "/home/hannes/Desktop/1331225538876_6396.arc.gz";

		File tempInputFile = new File(filename);

		Process p = Runtime.getRuntime().exec(
				new String[] { "gunzip", tempInputFile.toString() });
		try {
			p.waitFor();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		File unpackedFile = new File(tempInputFile.toString()
				.replace(".gz", ""));
		if (!unpackedFile.exists()) {
			log.warn("unable to find " + unpackedFile);
			System.exit(-1);
		}
		log.info("Unpacked to " + unpackedFile);
		Reader arcFileReader = new InputStreamReader(new FileInputStream(
				unpackedFile), "ASCII");

		new FileReader(unpackedFile);

		RDFExtractor extractor = new RDFExtractor(new FileOutputStream(
				File.createTempFile("foo", "bar")));
		StatHandler statHandler = new LoggingStatHandler();

		Map<String, String> dataStats = new UltraFailArcProcessor()
				.processUnpackedArcData(arcFileReader, filename, extractor,
						statHandler);
		log.info(dataStats);

	}

}
