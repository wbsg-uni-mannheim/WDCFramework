package org.webdatacommons.structureddata.tablegen;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Literal;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.RDF;
import org.eclipse.rdf4j.rio.ParseErrorListener;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.helpers.NTriplesParserSettings;
import org.eclipse.rdf4j.rio.nquads.NQuadsParser;

abstract class StreamingEntityReader {

	private static Logger log = Logger.getLogger(StreamingEntityReader.class);

	private int capacity = 0;
	private int headCap = 0;
	private long entitiesRead = 0;

	public long getEntitiesRead() {
		return entitiesRead;
	}

	private LinkedList<Statement> elements = new LinkedList<Statement>();
	private LinkedList<Statement> head = new LinkedList<Statement>();

	private Map<String, List<Statement>> index = new HashMap<String, List<Statement>>();

	public StreamingEntityReader(int capacity) {
		if (capacity < 10) {
			throw new IllegalArgumentException(
					"Capacity really should be >= 10.");
		}
		this.capacity = capacity;
		this.headCap = capacity / 2;
	}

	public void finish() {
		while (head.size() > 0) {
			handle(head.removeLast());
		}
	}

	public void add(Statement s) {
		elements.addFirst(s);
		head.addFirst(s);

		String indexKey = s.getSubject().toString();

		if (!index.containsKey(indexKey)) {
			index.put(indexKey, new ArrayList<Statement>());
		}
		index.get(indexKey).add(s);

		// select next elements to handle
		while (head.size() > headCap) {
			Statement visitStmt = head.removeLast();
			handle(visitStmt);
		}

		// throw out old elements
		while (elements.size() > capacity) {
			Statement removeStmt = elements.removeLast();
			index.remove(removeStmt.getSubject().toString());
		}
	}

	public void addNquads(Reader nQuadReader) throws RDFParseException,
			RDFHandlerException, IOException {
		
		//the map is no longer used
//		RDFParser p = new RobustNquadsParser() {
//			// overwrite, as original implementation kept a map with all bnode
//			// ids, which will overflow in our case.
//			protected BNode createBNode(String nodeID) throws RDFParseException {
//				return ValueFactoryImpl.getInstance().createBNode(nodeID);
//			}
//		};
//		p.setRDFHandler(new RDFHandlerBase() {
//			@Override
//			public void handleStatement(Statement arg0)
//					throws RDFHandlerException {
//				add(arg0);
//			}
//		});
		RDFParser p = new NQuadsParser();
		p.getParserConfig().set(NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES,
				Boolean.FALSE);
		p.setParseErrorListener(new ParseErrorListener() {
			@Override
			public void warning(String msg, long lineNo, long colNo) {
				log.debug("RDF parser warning: " + msg + " (" + lineNo + ":"
						+ colNo + ")");
			}

			@Override
			public void fatalError(String msg, long lineNo, long colNo) {
				log.debug("RDF parser fatal error: " + msg + " (" + lineNo
						+ ":" + colNo + ")");
			}

			@Override
			public void error(String msg, long lineNo, long colNo) {
				log.debug("RDF parser error: " + msg + " (" + lineNo + ":"
						+ colNo + ")");
			}

		});
		p.parse(nQuadReader, "p:base");
	}

	public static class Entity {

		private Map<String, String> properties;
		private String url;
		private String context;

		public Entity(String url, String context) {
			this.url = url;
			this.context = context;
		}

		public Entity(String url, String context, Map<String, String> properties) {
			this.url = url;
			this.context = context;

			this.properties = properties;
		}

		public Map<String, String> getProperties() {
			return properties;
		}

		public void setProperties(Map<String, String> properties) {
			this.properties = properties;
		}

		public String getUrl() {
			return url;
		}

		public void setUrl(String url) {
			this.url = url;
		}

		public String getContext() {
			return context;
		}

		public void setContext(String context) {
			this.context = context;
		}

		public boolean hasType() {
			return properties != null
					&& properties.containsKey(RDF.TYPE.toString());
		}

		public String getType() {
			if (properties.containsKey(RDF.TYPE.toString())) {
				return properties.get(RDF.TYPE.toString());
			} else {
				throw new IllegalArgumentException("Entity " + url
						+ " does not have a rdf:type property");
			}
		}

		public static final int VALUE_LENGTH = 100;

		@Override
		public String toString() {
			String ret = "<" + url + "> from <" + context + ">";
			if (properties != null) {
				for (Map.Entry<String, String> prop : properties.entrySet()) {
					String valueStr = prop.getValue().replace("\n", " ");
					if (valueStr.length() > VALUE_LENGTH) {
						valueStr = valueStr.substring(0, VALUE_LENGTH) + " ...";
					}
					ret += "\n\t" + prop.getKey() + " = " + valueStr;
				}
			}
			return ret;
		}
	}

	public static final int DEPTH_LIMIT = 3;

	public Entity resolveEntity(String key, int depth) {
		Map<String, String> entityProperties = new HashMap<String, String>();
		if (!hasEntry(key)) {
			return new Entity(key, "");
		}
		if (depth > DEPTH_LIMIT) {
			return new Entity(key, "");
		}
		String context = null;
		for (Statement s : rem(key)) {
			// this assumes entities only being described in a single context
			// TODO: do we want this?
			if (context == null) {
				context = s.getContext().toString();
			}
			if (s.getObject() instanceof Literal) {
				entityProperties.put(s.getPredicate().toString(),
						((Literal) s.getObject()).stringValue());
			}
			if (s.getObject() instanceof IRI) {
				entityProperties.put(s.getPredicate().toString(),
						((IRI) s.getObject()).toString());
			}
			// this flattens the object structure by adding child properties to
			// the main entity.
			if (s.getObject() instanceof BNode) {
				String objKey = s.getObject().toString();
				if (hasEntry(objKey) && !key.equals(objKey)) {
					entityProperties.putAll(resolveEntity(objKey, depth + 1)
							.getProperties());
				}
			}
		}
		entitiesRead++;
		return new Entity(key, context, entityProperties);
	}

	public List<Statement> rem(String key) {
		return index.remove(key);
	}

	public boolean hasEntry(String key) {
		return index.containsKey(key) && index.get(key).size() > 0;
	}

	public abstract void handle(Statement s);
}