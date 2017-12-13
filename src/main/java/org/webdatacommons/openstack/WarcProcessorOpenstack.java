package org.webdatacommons.openstack;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.archive.io.ArchiveReader;
import org.archive.io.ArchiveReaderFactory;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.commoncrawl.util.shared.FlexBuffer;
import org.jets3t.service.model.S3Object;
import org.webdatacommons.framework.io.CSVStatHandler;
import org.webdatacommons.framework.processor.FileProcessor;
import org.webdatacommons.framework.processor.ProcessingNode;
import org.webdatacommons.structureddata.extractor.RDFExtractor;
import org.webdatacommons.structureddata.extractor.RDFExtractor.ExtractorResult;
import org.webdatacommons.structureddata.util.WARCRecordUtils;

import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPResult;
import com.martiansoftware.jsap.UnflaggedOption;

/**
 * Uses the {@link org.archive.io.ArchiveReaderFactory} from the UKWA codebase
 * to read entries from WARC file. For each entry we then call the Any23 RDF
 * extractor and collect statistics.
 * Modified to output files on the computational instance and not on an object storage service
 * @author Anna Primpeli
 */
public class WarcProcessorOpenstack extends ProcessingNode implements FileProcessor {

	private static Logger log = Logger.getLogger(WarcProcessorOpenstack.class);

	// FIXME remove this if you do not want to get the links
	Pattern linkPattern = Pattern
		.compile(
				"<a[^>]+href=[\\\"']?([^\\\"']+wikipedia[^\\\"']+)[\"']?[^>]*>(.+?)</a>",
				Pattern.CASE_INSENSITIVE | Pattern.DOTALL);

	@Override
	public Map<String, String> process(ReadableByteChannel fileChannel, String inputFileKey) throws Exception {

		try {

			// create a tmp file to write the output for the triples to
			File tempOutputFile = File.createTempFile("dpef-triple-extraction", ".nq.gz");
			tempOutputFile.deleteOnExit();

			OutputStream tempOutputStream = new GZIPOutputStream(new FileOutputStream(tempOutputFile));
			RDFExtractor extractor = new RDFExtractor(tempOutputStream);

			// create file and stream for URLs.
			File tempOutputUrlFile = File.createTempFile("dpef-url-extraction", ".nq.gz");
			tempOutputUrlFile.deleteOnExit();

			BufferedWriter urlBW = new BufferedWriter(
					new OutputStreamWriter(new GZIPOutputStream(new FileOutputStream(tempOutputUrlFile)), "UTF-8"));

			
			// set name for data output
			String outputFileKey = "data/ex_" + inputFileKey.replace("/", "_") + ".nq.gz";
			// set name for stat output
			String outputStatsKey = "stats/ex_" + inputFileKey.replace("/", "_") + ".csv.gz";
			// set name for url output
			String outputUrlKey = "urls/ex_" + inputFileKey.replace("/", "_") + ".csv.gz";
			// set name for anchor output
			// String outputAnchorKey = "anchor/ex_"
			// + inputFileKey.replace("/", "_") + ".csv.gz";

			// default is false
			boolean logRegexError = Boolean.parseBoolean(getOrCry("logRegexFailures"));

			// get handler for page stats
			CSVStatHandler pageStatHandler = new CSVStatHandler();

			// get archive reader
			final ArchiveReader reader = ArchiveReaderFactory.get(inputFileKey, Channels.newInputStream(fileChannel),
					true);

			log.info("Extracting data from " + inputFileKey + " ...");

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
			// // number of anchors included in the pages
			// long anchorTotal = 0;
			// current time of the system when starting process.
			long start = System.currentTimeMillis();

			// TODO LOW write regex detection errors into SDB
			BufferedWriter bwriter = null;
			if (logRegexError) {
				FileWriter writer = new FileWriter(new File("regexFail_" + inputFileKey.replace("/", "_") + ".txt"));
				bwriter = new BufferedWriter(writer);
			}

			Iterator<ArchiveRecord> readerIt = reader.iterator();

			// read all entries in the ARC file
			while (readerIt.hasNext()) {

				ArchiveRecord record = readerIt.next();
				ArchiveRecordHeader header = record.getHeader();
				ArcFileItem item = new ArcFileItem();
				URI uri;

				item.setArcFileName(inputFileKey);

				// WARC contains lots of stuff. We only want HTTP responses
				if (!header.getMimetype().equals("application/http; msgtype=response")) {
					continue;
				}
				if (pagesTotal % 1000 == 0) {
					System.out.println(pagesTotal + " / " + pagesParsed + " / " 
							+ pagesTriples + " / " + pagesErrors);
					log.info(pagesTotal + " / " + pagesParsed + " / " 
					+ pagesTriples + " / " + pagesErrors);
				}
				//for testing purposes only and bwCloud restrictions
				if (pagesTotal==1000){
					System.out.println("Stop after parsing 1000 pages for testing purposes");
					break;
				} 
				try {

					uri = new URI(header.getUrl());
					String host = uri.getHost();
					// we only write if its valid
					if (host == null) {
						continue;
					} else {
						urlBW.write(uri.toString() + "\n");
					}
				} catch (URISyntaxException e) {
					log.error("Invalid URI!!!", e);
					continue;
				}

				String headers[] = WARCRecordUtils.getHeaders(record, true).split("\n");
				if (headers.length < 1) {
					pagesTotal++;
					continue;
				}

				// only consider HTML responses
				String contentType = headerKeyValue(headers, "Content-Type", "text/html");
				if (!contentType.contains("html")) {
					pagesTotal++;
					continue;
				}

				byte[] bytes = IOUtils.toByteArray(WARCRecordUtils.getPayload(record));

				if (bytes.length > 0) {

					item.setMimeType(contentType);
					item.setContent(new FlexBuffer(bytes, true));
					item.setUri(uri.toString());

					if (extractor.supports(item.getMimeType())) {
						// do extraction (woo ho)
						pagesParsed++;

						

						ExtractorResult result = extractor.extract(item);

						// if we had an error, increment error count
						if (result.hadError()) {
							pagesErrors++;
							pagesTotal++;
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
							stats.put("totalTriples", Long.toString(result.getTotalTriples()));

							if (result.getTotalTriples() > 0) {
								pagesTriples++;
							} else {
								log.debug("Could not find any triple in file, although guesser found something.");
								if (logRegexError) {
									String documentContent = item.getContent().toString("UTF-8");
									bwriter.write(
											"[Item without triple on position: " + item.getArcFilePos() + "]\n\n");
									for (String key : result.getReferencedData().keySet()) {
										bwriter.write(key + " : " + result.getReferencedData().get(key) + "\n");
									}
									bwriter.write("\n");
									bwriter.write(documentContent);
									bwriter.write("\n\n\n#########################\n\n\n");
								}
							}

							// write statistics about pages without errors but
							// not necessary with triples
							pageStatHandler.addStats(item.getUri(), stats);
							pagesGuessedTriples++;
						}
					}
					pagesTotal++;
				}
			}
			if (logRegexError) {
				bwriter.flush();
				try {
					bwriter.close();
				} catch (IOException e) {
					// do nothing;
				}
			}
			// we close the stream
			urlBW.close();
			// anchorBW.close();
			pageStatHandler.flush();
			// and the data stream
			// tempOutputStream.close();
			extractor.closeStream();

			/**
			 * write extraction results to the volume/ or to an external disk, if at least one included item was
			 * guessed to include triples
			 */
			System.out.println("Store extraction results");
			if (pagesGuessedTriples > 0) {
				if (!new File (getOrCry("outputDestination")+"data").isDirectory())
					if (!new File(getOrCry("outputDestination")+"data").mkdirs()) System.out.println("Paths were not formed correctly. Files will be saved in the tmp folder.");
				if (!new File (getOrCry("outputDestination")+"stats").isDirectory())
					if (!new File(getOrCry("outputDestination")+"stats").mkdirs()) System.out.println("Paths were not formed correctly. Files will be saved in the tmp folder.");				
				tempOutputFile.renameTo(new File(getOrCry("outputDestination")+outputFileKey));
												
				pageStatHandler.getFile().renameTo(new File(getOrCry("outputDestination")+outputStatsKey));
			}

			if (pagesTotal > 0) {
				if (!new File (getOrCry("outputDestination")+"urls").isDirectory())
					if (!new File(getOrCry("outputDestination")+"urls").mkdirs()) System.out.println("Paths were not formed correctly. Files will be saved in the tmp folder.") ;
				tempOutputUrlFile.renameTo(new File(getOrCry("outputDestination")+outputUrlKey));
			}

			double duration = (System.currentTimeMillis() - start) / 1000.0;
			double rate = (pagesTotal * 1.0) / duration;

			// create data file statistics and return
			Map<String, String> dataStats = new HashMap<String, String>();
			dataStats.put("duration", Double.toString(duration));
			dataStats.put("rate", Double.toString(rate));
			dataStats.put("pagesTotal", Long.toString(pagesTotal));
			dataStats.put("pagesParsed", Long.toString(pagesParsed));
			dataStats.put(PAGES_GUESSED_TRIPLES, Long.toString(pagesGuessedTriples));
			dataStats.put(PAGES_TRIPLES, Long.toString(pagesTriples));
			dataStats.put("pagesErrors", Long.toString(pagesErrors));

			log.info("Extracted data from " + inputFileKey + " - parsed " + pagesParsed + " pages in " + duration
					+ " seconds, " + rate + " pages/sec");

			reader.close();
			return dataStats;
		} catch (Exception e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
			throw new Exception(e.fillInStackTrace());
		}
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

	// some Hannes-TM HTTP header parsing kludges, way faster than libs
	public static String headerValue(String[] headers, String key, String dflt) {
		for (String hdrLine : headers) {
			if (hdrLine.toLowerCase().trim().startsWith(key.toLowerCase())) {
				return hdrLine.trim();
			}
		}
		return dflt;
	}

	public static String headerKeyValue(String[] headers, String key, String dflt) {
		String line = headerValue(headers, key, null);
		if (line == null)
			return dflt;
		String[] pces = line.split(":");
		if (pces.length != 2)
			return dflt;
		return pces[1].trim();
	}

	/**
	 * Main method to run the ARC extractor from the command line
	 * 
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		JSAP jsap = new JSAP();
		UnflaggedOption arcFileParam = new UnflaggedOption("arcfile").setStringParser(JSAP.STRING_PARSER)
				.setRequired(true).setGreedy(true);

		arcFileParam.setHelp("gzipped ARC files with web crawl data");

		jsap.registerParameter(arcFileParam);

		JSAPResult config = jsap.parse(args);

		if (!config.success()) {
			System.err.println("Usage: " + WarcProcessorOpenstack.class.getName() + " " + jsap.getUsage());
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

			new WarcProcessorOpenstack().process(Channels.newChannel(new FileInputStream(arcFile)), arcFile.toString());
		}
	}

}
