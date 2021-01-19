package org.webdatacommons.structureddata.iohandler;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.any23.extractor.ExtractionContext;
import org.apache.any23.writer.TripleHandler;
import org.apache.any23.writer.TripleHandlerException;
import org.apache.log4j.Logger;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.helpers.BasicWriterSettings;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;
import org.webdatacommons.structureddata.extractor.RDFExtractor;

public class FilterableTripleHandler implements TripleHandler {

    private static Logger log = Logger.getLogger(FilterableTripleHandler.class);


	private long totalTriples = 0;
	private Map<String, Long> triplesPerExtractor = new HashMap<String, Long>();
	private List<String> negativeFilterNamespaces;
	private List<String> positiveFilterNamespaces;
	private OutputStreamWriter writer;
	private boolean started = false;
	private Map<String, String> namespaceTable;

	/**
	 * Create a FilterableTripleHandler which allows the controlled exclusion of
	 * triples belonging to namespaces which are not wanted
	 * 
	 * @param osw
	 *            The {@link OutputStreamWriter}
	 * @param negativeFilterNamespaces
	 *            List of namespaces which should be left out
	 * @param positivFilterNamespaces
	 *            List of namesspaces within the negative namesspace list which
	 *            shouldbe still included
	 */
	public FilterableTripleHandler(OutputStreamWriter osw,
			List<String> negativeFilterNamespaces,
			List<String> positivFilterNamespaces) {
		this.writer = osw;
		this.negativeFilterNamespaces = negativeFilterNamespaces;
		this.positiveFilterNamespaces = positivFilterNamespaces;
		for (String ex : RDFExtractor.EXTRACTORS) {
			triplesPerExtractor.put(ex, new Long(0));
		}
	}

	Map<String, IRI> extractorNames = new HashMap<String, IRI>();

	public void receiveTriple(Resource s, IRI p, Value o, IRI g,
			ExtractionContext context) throws TripleHandlerException {
		// if uri is in negative namespace which has to be filtered out and not
		// in the positive list - return directly
		for (String negativeFilterNamespace : negativeFilterNamespaces) {
			if (p.toString().startsWith(negativeFilterNamespace)) {
				for (String positiveFilterNamespace : positiveFilterNamespaces) {
					if (!p.toString().startsWith(positiveFilterNamespace)) {
                            log.debug("Namespace filtered: "
                                    + s.toString() + " , " + p.toString() + ", "
                                    + o.toString());
						return;
					}
				}
			}
		}

		IRI extractorUri = extractorNames.get(context.getExtractorName());
		if (extractorUri == null) {
			extractorUri = SimpleValueFactory.getInstance().createIRI(
					"ex:" + context.getExtractorName());
			extractorNames.put(context.getExtractorName(), extractorUri);
		}

		try {
			handleStatement(s, p, o, context.getDocumentIRI(), extractorUri);
		} catch (RDFHandlerException e) {
			throw new TripleHandlerException("Unable to recieve Triple", e);
		}

		totalTriples++;
		String ex = context.getExtractorName();
		if (triplesPerExtractor.containsKey(ex)) {
			triplesPerExtractor.put(ex, new Long(
					triplesPerExtractor.get(ex) + 1));
		}
	}

	public long getTotalTriplesFound() {
		return totalTriples;
	}

	public Map<String, Long> getTriplesPerExtractor() {
		return triplesPerExtractor;
	}

	@Override
	public void startDocument(IRI documentIRI) throws TripleHandlerException {
		started = true;
	}

	@Override
	public void openContext(ExtractionContext context)
			throws TripleHandlerException {
		// ignore
	}

	@Override
	public void receiveNamespace(String prefix, String uri,
			ExtractionContext context) throws TripleHandlerException {
		if (!started) {
			throw new IllegalStateException("Parsing never started.");
		}

		if (namespaceTable == null) {
			namespaceTable = new HashMap<String, String>();
		}
		namespaceTable.put(prefix, NTriplesUtil.escapeString(uri));

	}

	public void handleStatement(Resource subject, IRI predicate, Value object,
			Resource... contexts) throws RDFHandlerException {
		if (!started) {
			throw new IllegalStateException(
					"Cannot handle statement without start parsing first.");
		}

		try {
			printResource(subject);
			printSpace();
			printIRI(predicate);
			printSpace();
			printObject(object);
			printSpace();

			for (int i = 0; i < contexts.length; i++) {
				printResource(contexts[i]);
				printSpace();
			}

			printCloseStatement();
		} catch (IOException ioe) {
			throw new RDFHandlerException(
					"An error occurred while printing statement.", ioe);
		}
	}

	@Override
	public void closeContext(ExtractionContext context)
			throws TripleHandlerException {
		// ignore
	}

	@Override
	public void endDocument(IRI documentIRI) throws TripleHandlerException {
		// ignore
	}

	@Override
	public void setContentLength(long contentLength) {
		// ignore

	}

	@Override
	public void close() throws TripleHandlerException {
		if (!started) {
			throw new IllegalStateException("Parsing never started.");
		}

		try {
			writer.flush();
		} catch (IOException ioe) {
			throw new TripleHandlerException("Error while flushing writer.",
					ioe);
		} finally {
			started = false;
			if (namespaceTable != null) {
				namespaceTable.clear();
			}
		}
	}

	private void printSpace() throws IOException {
		writer.append(' ');
	}

	private void printCloseStatement() throws IOException {
		writer.append(" .\n");
		writer.flush();
	}

	private void printIRI(IRI iri) throws IOException {
		final String iriString = iri.stringValue();
		int splitIdx = 0;
		String namespace = null;
		if (namespaceTable != null) {
			splitIdx = iriString.indexOf(':');
			if (splitIdx > 0) {
				String prefix = iriString.substring(0, splitIdx);
				namespace = namespaceTable.get(prefix);
			}
		}

		if (namespace != null) {
			writer.append('<');
			writer.append(namespace);
			writer.append(NTriplesUtil.escapeString(iriString
					.substring(splitIdx)));
			writer.append('>');
		} else {
			writer.append('<');
			writer.append(NTriplesUtil.escapeString(iriString));
			writer.append('>');
		}
	}

	private void printBNode(BNode b) throws IOException {
		writer.append(NTriplesUtil.toNTriplesString(b));
	}

	private void printResource(Resource r) throws IOException {
		if (r instanceof BNode) {
			printBNode((BNode) r);
		} else if (r instanceof IRI) {
			printIRI((IRI) r);
		} else {
			throw new IllegalStateException();
		}
	}

	private void printLiteral(Literal l) throws IOException {
			
		writer.append(NTriplesUtil.toNTriplesString(l));
	}

	private void printObject(Value v) throws IOException {
		if (v instanceof Resource) {
			printResource((Resource) v);
			return;
		}
		printLiteral((Literal) v);
	}
}
