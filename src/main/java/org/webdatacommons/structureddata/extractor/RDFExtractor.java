package org.webdatacommons.structureddata.extractor;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.any23.Any23;
import org.apache.any23.ExtractionReport;
import org.apache.any23.extractor.ExtractionException;
import org.apache.any23.extractor.ExtractionParameters;
import org.apache.any23.extractor.ExtractorFactory;
import org.apache.any23.extractor.ExtractorGroup;
import org.apache.any23.extractor.html.AdrExtractorFactory;
import org.apache.any23.extractor.html.EmbeddedJSONLDExtractorFactory;
import org.apache.any23.extractor.html.GeoExtractorFactory;
import org.apache.any23.extractor.html.HCalendarExtractorFactory;
import org.apache.any23.extractor.html.HCardExtractorFactory;
import org.apache.any23.extractor.html.HListingExtractorFactory;
import org.apache.any23.extractor.html.HRecipeExtractorFactory;
import org.apache.any23.extractor.html.HResumeExtractorFactory;
import org.apache.any23.extractor.html.HReviewExtractorFactory;
import org.apache.any23.extractor.html.SpeciesExtractorFactory;
import org.apache.any23.extractor.html.XFNExtractorFactory;
import org.apache.any23.extractor.html.microformats2.HAdrExtractorFactory;
import org.apache.any23.extractor.microdata.MicrodataExtractorFactory;
import org.apache.any23.extractor.rdf.JSONLDExtractorFactory;
import org.apache.any23.extractor.rdfa.RDFa11ExtractorFactory;
import org.apache.any23.extractor.rdfa.RDFaExtractorFactory;
import org.apache.any23.mime.MIMEType;
import org.apache.any23.source.ByteArrayDocumentSource;
import org.apache.any23.source.DocumentSource;
import org.apache.any23.vocab.HRecipe;
import org.apache.any23.vocab.SINDICE;
import org.apache.any23.vocab.XHTML;
import org.apache.log4j.Logger;
import org.commoncrawl.protocol.shared.ArcFileItem;
import org.webdatacommons.structureddata.iohandler.FilterableTripleHandler;

/**
 * Wraps a Any23 instance, defines a specific set of extractors, a list of
 * property namespaces to be filtered. It extracts RDF, collects several
 * statistics on the extraction process, and writes triples to files. For added
 * performance, regex guessers are used to find out whether the document
 * contains data for the specific formats at all.
 * 
 * @author Hannes Muehleisen (hannes@muehleisen.org)
 * @author Robert Meusel (robert@informatik.uni-mannheim.de)
 */
public class RDFExtractor {

	public final static List<String> EXTRACTORS = Arrays.asList("html-rdfa", "html-rdfa11", "html-microdata",
			"html-mf-geo", "html-mf-hcalendar", "html-mf-hcard", "html-mf-adr", "html-mf2-h-adr", "html-mf-hlisting", "html-mf-hresume",
			"html-mf-hreview", "html-mf-species", "html-mf-hrecipe", "html-mf-xfn", "rdf-jsonld", "html-embedded-jsonld");

	private static ExtractorGroup extractorGroup;
	static {
		List<ExtractorFactory<?>> factories = new ArrayList<ExtractorFactory<?>>();
		factories.add(new BaselessRDFaExtractorFactory());
		//use the BaselessRDFA Extraxtor to avoid extracting og header properties
		//factories.add(new RDFaExtractorFactory());
		// factories.add(new RDFa11ExtractorFactory());
		factories.add(new MicrodataExtractorFactory());
		factories.add(new GeoExtractorFactory());
		factories.add(new HCalendarExtractorFactory());
		factories.add(new HCardExtractorFactory());
		factories.add(new AdrExtractorFactory());
		factories.add(new HAdrExtractorFactory());
		factories.add(new HListingExtractorFactory());
		factories.add(new HResumeExtractorFactory());
		factories.add(new HReviewExtractorFactory());
		factories.add(new SpeciesExtractorFactory());
		// factories.add(new WDCHTMLMetaExtractorFactory());
		factories.add(new HRecipeExtractorFactory());
		factories.add(new XFNExtractorFactory());
		factories.add(new JSONLDExtractorFactory());
		factories.add(new EmbeddedJSONLDExtractorFactory());
		extractorGroup = new ExtractorGroup(factories);
	}

	public final static Map<String, Pattern> dataGuessers = new HashMap<String, Pattern>();
	static {
		Map<String, String> guessers = new HashMap<String, String>();

		// TODO check if we need to exclude the <meta beforehand to reduce the
		// number of false positives
		guessers.put("html-rdfa", "(property|typeof|about|resource)\\s*=");
		guessers.put("html-microdata", "(itemscope|itemprop\\s*=)");

		// TODO check if this is enough
		guessers.put("rdf-jsonld", "ld+json");

		// microdata guessers
		guessers.put("html-mf-geo", "class\\s*=\\s*(\"|')[^\"']*geo");
		guessers.put("html-mf-species", "class\\s*=\\s*(\"|')[^\"']*species");
		// this regex leads to some miss-detections but at the moment i have no
		// idea how to solve. e.g. home will be detected but no triple will be
		// extracted from Any23
		guessers.put("html-mf-xfn",
				"<a[^>]*rel\\s*=\\s*(\"|')[^\"']*(contact|acquaintance|friend|met|co-worker|colleague|co-resident|neighbor|child|parent|sibling|spouse|kin|muse|crush|date|sweetheart|me)");

		// following formats define unique enough main CSS class names
		guessers.put("html-mf-hcalendar", "(vcalendar|vevent)");
		guessers.put("html-mf-hcard", "vcard");
		guessers.put("html-mf-hlisting", "hlisting");
		guessers.put("html-mf-hresume", "hresume");
		guessers.put("html-mf-hreview", "hreview");
		guessers.put("html-mf-recipe", "hrecipe");

		for (Map.Entry<String, String> guesser : guessers.entrySet()) {
			dataGuessers.put(guesser.getKey(), Pattern.compile(guesser.getValue(), Pattern.CASE_INSENSITIVE));
		}
	}

	public final static Map<String, Pattern> formatGuessers = new HashMap<String, Pattern>();
	static {
		String rdfFormatsRegex = "((text/(rdf+nq|nq|nquads|n-quads|nt|ntriples|rdf|rdf+xml|rdf+n3|n3|turtle))|(application/(rdf+xml|rdf|n3|x-turtle|turtle)))";
		formatGuessers.put("scriptData", Pattern
				.compile("<script[^>]*type\\s*=\\s*(\"|')" + rdfFormatsRegex + "[^\"']*", Pattern.CASE_INSENSITIVE));
		formatGuessers.put("linkRelData", Pattern.compile("<link[^>]*type\\s*=\\s*(\"|')" + rdfFormatsRegex + "[^\"']*",
				Pattern.CASE_INSENSITIVE));
	}

	// exclude namesspaces which cause title and css links to be included as
	// triples
	public final static List<String> evilNamespaces = Arrays.asList(XHTML.NS, SINDICE.NS);
	// allow namespaces which are subsets of evilNamespaces to be included
	// TODO check if other namesspaces must be included as well here
	public final static List<String> notSoEvilNamespaces = Arrays.asList(HRecipe.NS);

	private static Logger log = Logger.getLogger(RDFExtractor.class);

	private Any23 any23Parser;
	ExtractionParameters any23ExParams;
	private OutputStreamWriter outputStreamWriter;

	public RDFExtractor(OutputStream output) throws UnsupportedEncodingException {
		any23ExParams = ExtractionParameters.newDefault();
		any23ExParams.setFlag("any23.extraction.metadata.timesize", false);
		any23ExParams.setFlag("any23.extraction.head.meta", false);

		any23Parser = new Any23(extractorGroup);

		this.outputStreamWriter = new OutputStreamWriter(output, "UTF-8");
	}
	
	public void closeStream(){
		try {
			outputStreamWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static class ExtractorResult {

		private static String REFERENCED_DATA_FORMAT = "referencedData";
		private String detectedMimeType = "";
		private boolean hadError = false;
		private Map<String, Long> extractorTriples = new HashMap<String, Long>();
		private String referencedData = "";
		private boolean hadResults = false;

		private long totalTriples = 0;

		public Map<String, String> getExtractorTriples() {
			Map<String, String> extractionStats = new HashMap<String, String>();
			for (Map.Entry<String, Long> statEntry : extractorTriples.entrySet()) {
				extractionStats.put(statEntry.getKey(), Long.toString(statEntry.getValue()));
			}
			return extractionStats;
		}

		/**
		 * Gets a Map with one entry which includes the referenced data format
		 * which was indicate to parse the file
		 * 
		 * @return Map where REFERENCED_DATA_FORMAT is the key and the data type
		 *         the value.
		 */
		public Map<String, String> getReferencedData() {
			Map<String, String> extractionStats = new HashMap<String, String>();
			extractionStats.put(REFERENCED_DATA_FORMAT, referencedData);
			return extractionStats;
		}

		public boolean hadResults() {
			return hadResults;
		}

		public boolean hadError() {
			return hadError;
		}

		public String getMimeType() {
			return detectedMimeType;
		}

		public long getTotalTriples() {
			return totalTriples;
		}
	}

	/**
	 * Checks if the documentContent is likely to be interesting for the
	 * extraction of structured data.
	 * 
	 * @param documentContent
	 *            content of the document
	 * @param result
	 *            the current extractur result
	 * @return true if structured data is likely to be included.
	 */
	private boolean interesting(String documentContent, ExtractorResult result) {
		// check if the document contains references to structured data,
		// e.g. as "<link rel"
		for (Map.Entry<String, Pattern> guesser : formatGuessers.entrySet()) {
			Matcher m = guesser.getValue().matcher(documentContent);
			boolean match = m.find();
			if (match) {
				result.referencedData = guesser.getKey();
				result.hadResults = true;
				return true;
			}
		}
		// check if document contains any structured data
		// possible extension: create ad-hoc extractor list from matches
		for (Map.Entry<String, Pattern> guesser : dataGuessers.entrySet()) {
			Matcher m = guesser.getValue().matcher(documentContent);
			boolean match = m.find();
			if (match) {
				result.referencedData = guesser.getKey();
				result.hadResults = true;
				return true;
			}
		}
		return false;
	}

	public ExtractorResult extract(ArcFileItem item) {
		ExtractorResult result = new ExtractorResult();

		try {
			String documentContent = item.getContent().toString("UTF-8");
			if (!interesting(documentContent, result)) {
				// if guessers do not match, return empty result
				return result;
			}

			DocumentSource any23Source = new ByteArrayDocumentSource(documentContent.getBytes("UTF-8"), item.getUri(),
					item.getMimeType());

			FilterableTripleHandler writer = new FilterableTripleHandler(outputStreamWriter, evilNamespaces,
					notSoEvilNamespaces);

			/**
			 * Call any23 extractor
			 */
			ExtractionReport report = any23Parser.extract(any23ExParams, any23Source, writer);

			result.detectedMimeType = report.getDetectedMimeType();
			result.totalTriples = writer.getTotalTriplesFound();
			result.extractorTriples = writer.getTriplesPerExtractor();

		} catch (Exception e) {
			if (log.isDebugEnabled()) {
				log.debug("Unable to parse " + item.getUri(), e);
			}
			result.hadError = true;
		}
		return result;
	}

	public boolean supports(String mimeType) {
		try {
			MIMEType type = MIMEType.parse(mimeType);
			return !extractorGroup.filterByMIMEType(type).isEmpty();
		} catch (IllegalArgumentException e) {
			return false;
		}
	}

	// Small test environment to test extractions
	@SuppressWarnings("unused")
	public static void main(String[] args) {
		List<ExtractorFactory<?>> factories = new ArrayList<ExtractorFactory<?>>();
		factories.add(new BaselessRDFaExtractorFactory());
		//use the BaselessRDFA Extraxtor to avoid extracting og header properties
		//factories.add(new RDFaExtractorFactory());
		//factories.add(new RDFa11ExtractorFactory());
		factories.add(new MicrodataExtractorFactory());
		factories.add(new GeoExtractorFactory());
		factories.add(new HCalendarExtractorFactory());
		factories.add(new HCardExtractorFactory());
		factories.add(new AdrExtractorFactory());
		factories.add(new HAdrExtractorFactory());

		factories.add(new HListingExtractorFactory());
		factories.add(new HResumeExtractorFactory());
		factories.add(new HReviewExtractorFactory());
		factories.add(new SpeciesExtractorFactory());
//		 factories.add(new WDCHTMLMetaExtractorFactory());
		factories.add(new HRecipeExtractorFactory());
		factories.add(new XFNExtractorFactory());
		factories.add(new JSONLDExtractorFactory());
		factories.add(new EmbeddedJSONLDExtractorFactory());


		Any23 any = new Any23(new ExtractorGroup(factories));
//		 Any23 any = new Any23();
//		 any.setHTTPUserAgent("Any23-Servlet");

		for (File input:new File("C:\\Users\\User\\workspace\\WDC_Extraction_2017\\src\\test\\java\\org\\webdatacommons\\structureddata\\test\\exampledata").listFiles()) {
			
			System.out.println("Extract triples from: "+input.getName());
			DocumentSource any23Source = new ByteArrayDocumentSource(
					readDocument(input.getPath()).getBytes(), "http://www.test.de", "text/html");
	
			File f = new File("C:\\Users\\User\\workspace\\WDC_Extraction_2017\\src\\test\\resources\\test_2020\\results_any23vs2.4\\"+input.getName().replace(".html", ".nq"));
	
			OutputStreamWriter w;
			try {
				w = new OutputStreamWriter(new FileOutputStream(f));
				FilterableTripleHandler writer = new FilterableTripleHandler(w, evilNamespaces, notSoEvilNamespaces);
	
				ExtractionParameters any23Params = ExtractionParameters.newDefault();
				any23Params.setFlag("any23.extraction.metadata.timesize", false);
				any23Params.setFlag("any23.extraction.head.meta", false);
				ExtractionReport report = any.extract(any23Params, any23Source, writer);
				System.out.println("Triples:"+writer.getTotalTriplesFound());
				w.flush();
				w.close();
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ExtractionException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

	}

	private static String readDocument(String file) {
		StringBuilder sb = new StringBuilder();
		BufferedReader bf;
		try {
			bf = new BufferedReader(new FileReader(new File(file)));
			while (bf.ready()) {
				sb.append(bf.readLine());
			}
			bf.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return sb.toString();
	}

}
