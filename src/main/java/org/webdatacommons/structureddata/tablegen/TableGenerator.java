package org.webdatacommons.structureddata.tablegen;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.webdatacommons.framework.io.CSVExport;

import au.com.bytecode.opencsv.CSVWriter;

import com.martiansoftware.jsap.FlaggedOption;
import com.martiansoftware.jsap.JSAP;
import com.martiansoftware.jsap.JSAPException;
import com.martiansoftware.jsap.JSAPResult;

public abstract class TableGenerator {

	private static final int CACHE_SIZE = 100;

	private static Logger log = Logger.getLogger(TableGenerator.class);

	protected abstract String[] getTableHeader();

	protected abstract boolean triggerEntity(Statement stmt);

	protected abstract boolean acceptResult(Map<String, String> entityProperties);

	public void generateTable(File in, File out) throws RDFParseException,
			RDFHandlerException, FileNotFoundException, IOException {

		final CSVWriter writer = new CSVWriter(new FileWriter(out), ',');
		writer.writeNext(getTableHeader());

		final StreamingEntityReader rc = new StreamingEntityReader(CACHE_SIZE) {
			@Override
			public void handle(Statement stmt) {
				if (triggerEntity(stmt)) {
					Map<String, String> entityProperties = resolveEntity(
							stmt.getSubject().toString(),0).getProperties();
					if (entityProperties.size() > 0
							&& acceptResult(entityProperties)) {

						List<String> line = new ArrayList<String>();
						for (String key : getTableHeader()) {
							if (entityProperties.containsKey(key)) {
								line.add(entityProperties.get(key));
							} else {
								line.add("");
							}
						}
						writer.writeNext(line.toArray(new String[0]));
					}
				}
			}
		};

		log.info("Reading "
				+ CSVExport.humanReadableByteCount(in.length(), false)
				+ " of input...");
		rc.addNquads(new FileReader(in));
		rc.finish();
		writer.close();
		log.info("done.");
	}

	protected boolean empty(String value) {
		if (value == null) {
			return true;
		}
		return "".equals(value.trim());
	}

	public static void main(String[] args) throws IOException, JSAPException {
		JSAP jsap = new JSAP();
		FlaggedOption inFileP = new FlaggedOption("nqfile")
				.setStringParser(JSAP.STRING_PARSER).setRequired(true)
				.setLongFlag("input-file").setShortFlag('i');
		inFileP.setHelp("RDF file in NQuads format");
		jsap.registerParameter(inFileP);

		FlaggedOption inFormat = new FlaggedOption("format")
				.setStringParser(JSAP.STRING_PARSER).setRequired(true)
				.setLongFlag("format").setShortFlag('f');
		inFormat.setHelp("microformats type, e.g. 'hcard'");
		jsap.registerParameter(inFormat);

		JSAPResult config = jsap.parse(args);

		if (!config.success()) {
			System.err.println("Usage: " + TableGenerator.class.getName() + " "
					+ jsap.getUsage());
			System.err.println(jsap.getHelp());
			System.exit(1);
		}

		File inFile = new File(config.getString("nqfile"));
		if (!inFile.exists() || !inFile.canRead()) {
			System.err.println("Unable to open " + inFile);
			System.exit(1);
		}

		try {
			if ("hcard".equals(config.getString("format"))) {
				new HCardGenerator().generateTable(inFile, new File(inFile
						+ ".csv"));
			}
		} catch (Exception e) {
			log.warn("Unable to parse " + inFile, e);
		}
	}
}
