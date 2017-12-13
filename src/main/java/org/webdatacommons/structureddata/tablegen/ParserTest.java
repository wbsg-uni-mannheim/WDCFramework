package org.webdatacommons.structureddata.tablegen;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.log4j.Logger;
import org.apache.xerces.util.ParserConfigurationSettings;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.ParserConfig;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.helpers.NTriplesParserSettings;

public class ParserTest {
	
	private static Logger log = Logger.getLogger(ParserTest.class);


	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws RDFParseException,
			RDFHandlerException, FileNotFoundException, IOException {
		// TODO Auto-generated method stub
		RDFParser p = new RobustNquadsParser();
		//RDFParser p = new NQuadsParser();
		p.getParserConfig().set(NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES,
				Boolean.FALSE);

		p.setRDFHandler(new RDFHandler() {
			
			@Override
			public void startRDF() throws RDFHandlerException {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void handleStatement(Statement st) throws RDFHandlerException {
				log.info(st);
			}
			
			@Override
			public void handleNamespace(String prefix, String uri)
					throws RDFHandlerException {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void handleComment(String comment) throws RDFHandlerException {
				// TODO Auto-generated method stub
				
			}
			
			@Override
			public void endRDF() throws RDFHandlerException {
				// TODO Auto-generated method stub
				
			}
		});
		p.parse(new FileReader(new File("/home/hannes/Desktop/foo/tailfail.nq")),
				"foo://");
	}

}
