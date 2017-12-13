package org.webdatacommons.structureddata.processor;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.commoncrawl.util.shared.ArcFileReader;
import org.webdatacommons.framework.io.LoggingStatHandler;
import org.webdatacommons.framework.io.StatHandler;
import org.webdatacommons.structureddata.extractor.RDFExtractor;
import org.webdatacommons.structureddata.extractor.RDFExtractor.ExtractorResult;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.UnflaggedOption;

/**
 * Uses the {@link org.commoncrawl.util.shared.ArcFileReader.ArcFileReader} from
 * the commoncrawl codebase to read entries from a gzipped ARC file. For each
 * entry we then call the Any23 RDF extractor and collect statistics.
 * 
 * @author Hannes Muehleisen (hannes@muehleisen.org)
 */
//TODO LOW move this to the new structure and let it implement FileProcessor
public class ArcProcessor {
	private static Logger log = Logger.getLogger(ArcProcessor.class);

	private final static int BLOCKSIZE = 4096;

	public Map<String, String> processArcData(
			final ReadableByteChannel gzippedArcFileBC, String arcFileName,
			RDFExtractor extractor, StatHandler statHandler,
			boolean logRegexErrors) throws IOException {

		log.info("Extracting data from " + arcFileName + " ...");
		final ArcFileReader reader = new ArcFileReader();
		/**
		 * This thread asynchronously copies the data from the gzipped arc file
		 * into the input buffer of the ARC file reader. The reader will block
		 * once its buffers are full, and enables access the ARC file entries.
		 * */
		(new Thread() {
			public void run() {
				try {
					while (true) {
						ByteBuffer buffer = ByteBuffer.allocate(BLOCKSIZE);
						int bytesRead = gzippedArcFileBC.read(buffer);

						if (bytesRead > 0) {
							buffer.flip();
							reader.available(buffer);
						} else if (bytesRead == -1) {
							reader.finished();
							return;
						}
					}
				} catch (IOException e) {
					log.warn("Unable to pipe input to reader");
					reader.finished();
					return;
				}
			}
		}).start();

		// number of pages visited for extraction
		long pagesTotal = 0;
		// number of pages parsed based on supported mime-type
		long pagesParsed = 0;
		// number of pages which contain an error
		long pagesErrors = 0;
		// number of pages which are likely to include triples
		long pagesGuessedTriples = 0;
		// number of pages including at least one triple
		long pagesTriples = 0;
		// current time of the system when starting process.
		long start = System.currentTimeMillis();

		// TODO LOW write regex detection errors into SDB
		BufferedWriter bwriter = null;
		if (logRegexErrors) {
			FileWriter writer = new FileWriter(new File("regexFail_"
					+ arcFileName.replace("/", "_") + ".txt"));
			bwriter = new BufferedWriter(writer);
		}

		// read all entries in the ARC file
		while (reader.hasMoreItems()) {
			log.info(pagesTotal + " / " + pagesErrors);
			ArcFileItem item = new ArcFileItem();
			item.setArcFileName(arcFileName);

			try {
				reader.getNextItem(item);
			} catch (Exception e) {
				log.warn(e, e.fillInStackTrace());
				pagesErrors++;
				continue;
			}

			// log.info(item.getContent().toString("UTF-8").substring(0,500));

			if (extractor.supports(item.getMimeType())) {
				// do extraction (woo ho)
				pagesParsed++;

				ExtractorResult result = extractor.extract(item);

				// if we had an error, increment error count
				if (result.hadError()) {
					pagesErrors++;
					continue;
				}
				// if we found no triples, continue
				if (result.hadResults()) {
					// collect some other statistics
					Map<String, String> stats = new HashMap<String, String>();
					stats.putAll(itemStats(item));
					stats.putAll(result.getExtractorTriples());
					stats.putAll(result.getReferencedData());
					stats.put("detectedMimeType", result.getMimeType());
					stats.put("totalTriples",
							Long.toString(result.getTotalTriples()));

					if (result.getTotalTriples() > 0) {
						pagesTriples++;
					} else {
						log.warn("Could not find any triple in file, although guesser found something.");
						if (logRegexErrors) {
							String documentContent = item.getContent()
									.toString("UTF-8");
							bwriter.write("[Item without triple on position: "
									+ item.getArcFilePos() + "]\n\n");
							for (String key : result.getReferencedData()
									.keySet()) {
								bwriter.write(key + " : "
										+ result.getReferencedData().get(key)
										+ "\n");
							}
							bwriter.write("\n");
							bwriter.write(documentContent);
							bwriter.write("\n\n\n#########################\n\n\n");
						}
					}

					// write statistics
					statHandler.addStats(item.getUri(), stats);
					pagesGuessedTriples++;
				}
			}
			pagesTotal++;

		}
		if (logRegexErrors) {
			bwriter.flush();
			try {
				bwriter.close();
			} catch (IOException e) {
				// do nothing;
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
		dataStats
				.put(PAGES_GUESSED_TRIPLES, Long.toString(pagesGuessedTriples));
		dataStats.put(PAGES_TRIPLES, Long.toString(pagesTriples));
		dataStats.put("pagesErrors", Long.toString(pagesErrors));

		log.info("Extracted data from " + arcFileName + " - parsed "
				+ pagesTotal + " pages in " + duration + " seconds, " + rate
				+ " pages/sec");

		reader.close();
		return dataStats;
	}

	public static final String PAGES_GUESSED_TRIPLES = "pagesGuessedTriples";
	public static final String PAGES_TRIPLES = "pagesTriples";

	protected static Map<String, String> itemStats(ArcFileItem item) {
		Map<String, String> stats = new HashMap<String, String>();
		// data about the file where this data came from
		stats.put("arcFileName", item.getArcFileName());
		stats.put("arcFilePos", Integer.toString(item.getArcFilePos()));
		stats.put("recordLength", Integer.toString(item.getRecordLength()));

		// data about the web page crawled
		stats.put("uri", item.getUri());
		stats.put("hostIp", item.getHostIP());
		stats.put("timestamp", Long.toString(item.getTimestamp()));
		stats.put("mimeType", item.getMimeType());
		return stats;
	}

	/**
	 * Main method to run the ARC extractor from the command line
	 * */
	public static void main(String[] args) throws IOException, JSAPException {
		JSAP jsap = new JSAP();
		UnflaggedOption arcFileParam = new UnflaggedOption("arcfile")
				.setStringParser(JSAP.STRING_PARSER).setRequired(true)
				.setGreedy(true);

		arcFileParam.setHelp("gzipped ARC files with web crawl data");

		jsap.registerParameter(arcFileParam);

		JSAPResult config = jsap.parse(args);

		if (!config.success()) {
			System.err.println("Usage: " + ArcProcessor.class.getName() + " "
					+ jsap.getUsage());
			System.err.println(jsap.getHelp());
			System.exit(1);
		}

		List<String> arcFiles = Arrays.asList(config.getStringArray("arcfile"));
		for (String arcFileS : arcFiles) {
			File arcFile = new File(arcFileS);
			if (!arcFile.exists() || !arcFile.canRead()) {
				System.err.println("Unable to open " + arcFile);
				continue;
			}

			File quadFile = new File(arcFile.toString() + ".nq");

			RDFExtractor extractor = new RDFExtractor(new FileOutputStream(
					quadFile));

			StatHandler pageStatHandler = new LoggingStatHandler();

			new ArcProcessor().processArcData(
					Channels.newChannel(new FileInputStream(arcFile)),
					arcFile.toString(), extractor, pageStatHandler, false);
		}
	}
}
