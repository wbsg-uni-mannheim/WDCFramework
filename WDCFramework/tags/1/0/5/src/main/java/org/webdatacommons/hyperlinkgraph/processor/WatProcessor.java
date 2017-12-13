package org.webdatacommons.hyperlinkgraph.processor;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

import org.apache.log4j.Logger;
import org.archive.io.ArchiveRecord;
import org.archive.io.ArchiveRecordHeader;
import org.archive.io.warc.WARCReader;
import org.archive.io.warc.WARCReaderFactory;
import org.archive.io.warc.WARCRecord;
import org.jets3t.service.model.S3Object;
import org.webdatacommons.cc.wat.json.WatJsonReader;
import org.webdatacommons.cc.wat.json.model.JsonData;
import org.webdatacommons.cc.wat.json.model.Link;
import org.webdatacommons.framework.io.CSVStatHandler;
import org.webdatacommons.framework.processor.FileProcessor;
import org.webdatacommons.framework.processor.ProcessingNode;

public class WatProcessor extends ProcessingNode implements FileProcessor{

	private static Logger log = Logger.getLogger(WatProcessor.class);

	@Override
	public Map<String, String> process(ReadableByteChannel fileChannel,
			String inputFileKey) throws Exception {

		// create an file writer for three hundreds found in the files.
		String outputThreehundredKey = "threehundred/ex_"
				+ inputFileKey.replace("/", "_") + ".csv.gz";
		CSVStatHandler threehundredHandler = new CSVStatHandler();

		// create an tmp output file for the extracted data
		File tempOutputFile = File.createTempFile(
				"cc-hyperlinkgraph-extraction", ".tab.gz");
		tempOutputFile.deleteOnExit();
		String outputFileKey = "data/ex_" + inputFileKey.replace("/", "_")
				+ ".sparse.gz";
		BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(
				new GZIPOutputStream(new FileOutputStream(tempOutputFile))));

		// number of responses in the file
		long responsesTotal = 0;
		// number of 200 in the file
		long twohundredTotal = 0;
		// number of 3xx in the file
		long threehundredTotal = 0;
		// text/html in the file
		long textTotal = 0;
		// number of links in the file
		long linksTotal = 0;
		// href links in the file
		long hrefTotal = 0;
		// current time of the system when starting process.
		long start = System.currentTimeMillis();
		// errors
		long errorTotal = 0;

		final WARCReader reader = (WARCReader) WARCReaderFactory.get(
				inputFileKey, Channels.newInputStream(fileChannel), true);
		// iterate over each record in the stream
		Iterator<ArchiveRecord> readerIt = reader.iterator();
		while (readerIt.hasNext()) {

			WARCRecord record = (WARCRecord) readerIt.next();
			BufferedReader br = new BufferedReader(new InputStreamReader(
					new BufferedInputStream(record)));
			try {
				while (br.ready()) {

					// in most cases we will only get the JSON here. The header
					// is
					// separated before hand
					String line = br.readLine();
					if (line.startsWith("{")) {

						JsonData jd = WatJsonReader.read(line);
						// check if its an response
						if (!jd.envelope.warcHeaderMetadata.warcType
								.equals("response")) {
							continue;
						}

						if (responsesTotal % 1000 == 0) {
							log.info(responsesTotal + " / " + errorTotal
									+ " / " + twohundredTotal + " / "
									+ threehundredTotal + " / " + textTotal
									+ " / " + linksTotal + " / " + hrefTotal);
						}
						responsesTotal++;
						// check for 200 status - most cases have this as
						// redirects
						// are not really common.
						if (jd.envelope.payLoadMetadata.httpResponseMetadata.responseMessage.status != 200) {
							// check for 3xx just for stats
							if (jd.envelope.payLoadMetadata.httpResponseMetadata.responseMessage.status >= 300
									&& jd.envelope.payLoadMetadata.httpResponseMetadata.responseMessage.status < 400) {
								threehundredTotal++;
								ArchiveRecordHeader header = record.getHeader();
								Map<String, String> threeHundredFile = new HashMap<String, String>();
								threeHundredFile.put("1uri", header.getUrl());
								threeHundredFile.put("2json", line);
								// write statistics
								threehundredHandler.addStats(header.getUrl(),
										threeHundredFile);
							}
							continue;
						}
						twohundredTotal++;
						// we only want text/html pages
						if (jd.envelope.payLoadMetadata.httpResponseMetadata.headers.contentType == null
								|| !jd.envelope.payLoadMetadata.httpResponseMetadata.headers.contentType
										.startsWith("text/html")) {
							continue;
						}
						textTotal++;
						// now we go through all links, if there are any
						if (jd.envelope.payLoadMetadata.httpResponseMetadata.htmlMetadata.links != null
								&& jd.envelope.payLoadMetadata.httpResponseMetadata.htmlMetadata.links.length > 0) {
							ArchiveRecordHeader header = record.getHeader();
							StringBuilder sb = new StringBuilder();
							sb.append(escape(header.getUrl()));
							for (Link link : jd.envelope.payLoadMetadata.httpResponseMetadata.htmlMetadata.links) {
								if (link != null && link.path != null) {
									// only hrefs
									if (link.path.indexOf("href") > -1) {
										sb.append("\t");
										sb.append(escape(link.url));
										hrefTotal++;
									}
									linksTotal++;
								}
							}
							sb.append("\n");
							// write to stream
							bw.write(sb.toString());
						}
						// stop if we found the first
						break;
					}

				}
			} catch (Exception ex) {
				log.error(ex + " in " + inputFileKey + " for record "
						+ record.getHeader().getUrl(), ex.fillInStackTrace());
				ex.printStackTrace();
				errorTotal++;
			} finally {
				br.close();
			}

		}
		// close the stream
		bw.close();
		
		// check if at least one 200 pages with text was found
		if (textTotal > 0) {
			S3Object dataFileObject = new S3Object(tempOutputFile);
			dataFileObject.setKey(outputFileKey);
			getStorage().putObject(getOrCry("resultBucket"), dataFileObject);
		}
		// check if at least on 3xx page was found
		if (threehundredTotal > 0) {
			S3Object statsFileObject = new S3Object(
					threehundredHandler.getFile());
			statsFileObject.setKey(outputThreehundredKey);
			getStorage().putObject(getOrCry("resultBucket"), statsFileObject);
		}

		// runtime and rate calculation
		double duration = (System.currentTimeMillis() - start) / 1000.0;
		double rate = (responsesTotal * 1.0) / duration;

		// create data file statistics and return
		Map<String, String> dataStats = new HashMap<String, String>();
		dataStats.put("duration", Double.toString(duration));
		dataStats.put("rate", Double.toString(rate));
		dataStats.put("responsesTotal", Long.toString(responsesTotal));
		dataStats.put("twohundredTotal", Long.toString(twohundredTotal));
		dataStats.put("threehundredTotal", Long.toString(threehundredTotal));
		dataStats.put("textTotal", Long.toString(textTotal));
		dataStats.put("linksTotal", Long.toString(linksTotal));
		dataStats.put("hrefTotal", Long.toString(hrefTotal));
		dataStats.put("errorTotal", Long.toString(errorTotal));

		return dataStats;
	}

	// urls could contain \n and we need to escape this
	private static String escape(String input) {
		if (input != null) {
			return input.replace("\\", "\\\\");
		}
		return null;
	}

}
