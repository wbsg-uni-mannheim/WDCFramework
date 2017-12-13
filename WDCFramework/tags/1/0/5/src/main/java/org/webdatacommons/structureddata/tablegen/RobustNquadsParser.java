/**
 * Copyright 2008-2010 Digital Enterprise Research Institute (DERI)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.webdatacommons.structureddata.tablegen;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.nio.charset.Charset;

import org.apache.any23.util.ReaderInputStream;
import org.apache.log4j.Logger;
import org.eclipse.rdf4j.common.lang.service.FileFormatServiceRegistry;
import org.eclipse.rdf4j.common.lang.service.ServiceRegistry;
import org.eclipse.rdf4j.model.BNode;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.Value;
import org.eclipse.rdf4j.model.datatypes.XMLDatatypeUtil;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.ParseLocationListener;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandler;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.helpers.AbstractRDFParser;
import org.eclipse.rdf4j.rio.helpers.BasicParserSettings;
import org.eclipse.rdf4j.rio.helpers.NTriplesParserSettings;
import org.eclipse.rdf4j.rio.helpers.RioSettingImpl;
import org.eclipse.rdf4j.rio.ntriples.NTriplesUtil;


/**
 * <i>N-Quads</i> parser implementation based on the
 * {@link org.openrdf.rio.RDFParser} interface. See the format specification <a
 * href="http://sw.deri.org/2008/07/n-quads/">here</a>.
 * 
 * Parser was extended to honor the stopAtFirstError flag. If the flag is set,
 * broken lines are skipped.
 * 
 * @author Michele Mostarda (mostarda@fbk.eu)
 * @author Hannes Muehleisen (hannes@muehleisen.org)
 * @see org.openrdf.rio.RDFParser
 */

//TODO: switch to native any23 NquadsParser

public class RobustNquadsParser extends AbstractRDFParser {

	private static Logger log = Logger.getLogger(RobustNquadsParser.class);

	/**
	 * Location listener acquired when parsing started.
	 */
	private ParseLocationListener locationListener;

	/**
	 * RDF handler acquired when parsing started.
	 */
	private RDFHandler rdfHandler;

	/**
	 * Current row, col and marker trackers.
	 */
	private int row, col, mark;

	public RobustNquadsParser() {
	}

	public RDFFormat getRDFFormat() {
		
		return RDFFormat.NQUADS;
		//return RDFFormat.register("N-Quads", "text/nquads", "nq", Charset.forName("UTF-8"));
	}

	public void parse(Reader reader, String s) throws IOException,
			RDFParseException, RDFHandlerException {
		ReaderInputStream readerInputStream = new ReaderInputStream(reader);
		parse(readerInputStream, s);
	}

	public synchronized void parse(InputStream is, String baseURI)
			throws IOException, RDFParseException, RDFHandlerException {
		if (is == null) {
			throw new NullPointerException("inputStream cannot be null.");
		}
		if (baseURI == null) {
			throw new NullPointerException("baseURI cannot be null.");
		}

		try {
			row = col = 1;

			locationListener = getParseLocationListener();
			rdfHandler = getRDFHandler();

			setBaseURI(baseURI);

			final BufferedReader br = new BufferedReader(new InputStreamReader(
					is));
			if (rdfHandler != null) {
				rdfHandler.startRDF();
			}
			while (parseLine(br)) {
				nextRow();
			}
		} finally {
			if (rdfHandler != null) {
				rdfHandler.endRDF();
			}
			clear();
		}
	}

	/**
	 * Moves to the next row, resets the column.
	 */
	private void nextRow() {
		col = 0;
		row++;
		if (locationListener != null) {
			locationListener.parseLocationUpdate(row, col);
		}
	}

	/**
	 * Moves to the next column.
	 */
	private void nextCol() {
		col++;
		if (locationListener != null) {
			locationListener.parseLocationUpdate(row, col);
		}
	}

	/**
	 * Reads the next char.
	 * 
	 * @param br
	 * @return the next read char.
	 * @throws IOException
	 */
	private char readChar(BufferedReader br) throws IOException {
		final int c = br.read();
		if (c == -1) {
			throw new EOS();
		}
		nextCol();
		return (char) c;
	}

	/**
	 * Reads an unicode char with pattern <code>\\uABCD</code>.
	 * 
	 * @param br
	 *            input reader.
	 * @return read char.
	 * @throws IOException
	 * @throws RDFParseException
	 */
	private char readUnicode(BufferedReader br) throws IOException,
			RDFParseException {
		final char[] unicodeSequence = new char[4];
		for (int i = 0; i < unicodeSequence.length; i++) {
			unicodeSequence[i] = readChar(br);
		}
		final String unicodeCharStr = new String(unicodeSequence);
		try {
			return (char) Integer.parseInt(unicodeCharStr, 16);
		} catch (NumberFormatException nfe) {
            reportError("Error while converting unicode char '\\u"
                    + unicodeCharStr + "'", row, col, new RioSettingImpl<Boolean>("","",true));
			throw new IllegalStateException();
		}
	}

	/**
	 * Marks the buffered input stream with the current location.
	 * 
	 * @param br
	 */
	private void mark(BufferedReader br) throws IOException {
		mark = col;
		br.mark(5);
	}

	/**
	 * Resets the buffered input stream and update the new location.
	 * 
	 * @param br
	 * @throws IOException
	 */
	private void reset(BufferedReader br) throws IOException {
		col = mark;
		br.reset();
		if (locationListener != null) {
			locationListener.parseLocationUpdate(row, col);
		}
	}

	/**
	 * Asserts to read a specific char.
	 * 
	 * @param br
	 * @param c
	 * @throws IOException
	 */
	private void assertChar(BufferedReader br, char c) throws IOException {
		char r = readChar(br);
		if (r != c) {
			throw new IllegalArgumentException(
					String.format(
							"Unexpected char at location %s %s, expected '%s', found '%s'",
							row, col, c, r));
		}
	}

	/**
	 * Parsers an <i>NQuads</i> line.
	 * 
	 * @param br
	 *            input stream reader containing NQuads.
	 * @return <code>false</code> if the parsing completed, <code>true</code>
	 *         otherwise.
	 * @throws IOException
	 * @throws RDFParseException
	 * @throws RDFHandlerException
	 */
	private boolean parseLine(BufferedReader br) throws IOException,
			RDFParseException, RDFHandlerException {

		if (!consumeSpacesAndNotEOS(br)) {
			return false;
		}

		// Consumes empty line or line comment.
		try {
			if (consumeEmptyLine(br))
				return true;
			if (consumeComment(br))
				return true;
		} catch (EOS eos) {
			return false;
		}

		final Resource sub;
		final IRI pred;
		final Value obj;
		final IRI graph;
		try {
			sub = parseSubject(br);
			consumeSpaces(br);
			pred = parsePredicate(br);
			consumeSpaces(br);
			obj = parseObject(br);
			consumeSpaces(br);
			graph = parseGraph(br);
			consumeSpaces(br);
			parseDot(br);
			notifyStatement(sub, pred, obj, graph);
		} catch (Exception iae) {
			if (NTriplesParserSettings.FAIL_ON_NTRIPLES_INVALID_LINES.equals(false)) {
				// remove remainder of broken line
				consumeBrokenLine(br);
				// notify parse error listener
				reportError(iae.getMessage(), row, col, new RioSettingImpl<Boolean>("","",true));
				return true;
			} else {
				throw new RDFParseException(iae);
			}
		}

		if (!consumeSpacesAndNotEOS(br)) {
			return false;
		}
		char c = readChar(br);
		boolean newline = (c == '\n');
		if (!newline) {
			consumeBrokenLine(br);
			reportError("Newline expected, but " + c + " found", row, col, new RioSettingImpl<Boolean>("","",true));
			return true;
		}
		return newline;
	}

	/**
	 * Consumes the remainder of a line where parsing has failed.
	 * 
	 * @param br
	 *            input NQuads stream.
	 * @throws IOException
	 *             if an error occurs while consuming stream.
	 */
	private void consumeBrokenLine(BufferedReader br) throws IOException {
		char c;
		while (true) {
			try {
			c = readChar(br);
			if (c == '\n') {
				mark(br);
				return;
			}} catch (EOS e) {
				return;
			}
		}
	}

	/**
	 * Consumes the line if empty (contains just a carriage return).
	 * 
	 * @param br
	 *            input NQuads stream.
	 * @return <code>true</code> if the line is empty.
	 * @throws IOException
	 *             if an error occurs while consuming stream.
	 */
	private boolean consumeEmptyLine(BufferedReader br) throws IOException {
		char c;
		mark(br);
		c = readChar(br);
		if (c == '\n') {
			return true;
		} else {
			reset(br);
			return false;
		}
	}

	/**
	 * Consumes all subsequent spaces and returns true, if End Of Stream is
	 * reached instead returns false.
	 * 
	 * @param br
	 *            input NQuads stream reader.
	 * @return <code>true</code> if there are other chars to be consumed.
	 * @throws IOException
	 *             if an error occurs while consuming stream.
	 */
	private boolean consumeSpacesAndNotEOS(BufferedReader br)
			throws IOException {
		try {
			consumeSpaces(br);
			return true;
		} catch (EOS eos) {
			return false;
		}
	}

	/**
	 * Consumes a comment if any.
	 * 
	 * @param br
	 *            input NQuads stream reader.
	 * @return <code>true</code> if comment has been consumed, false otherwise.
	 * @throws IOException
	 */
	private boolean consumeComment(BufferedReader br) throws IOException {
		char c;
		mark(br);
		c = readChar(br);
		if (c == '#') {
			mark(br);
			while (readChar(br) != '\n')
				;
			mark(br);
			return true;
		} else {
			reset(br);
			return false;
		}
	}

	/**
	 * Notifies the parsed statement to the {@link RDFHandler}.
	 * 
	 * @param sub
	 * @param pred
	 * @param obj
	 * @param graph
	 * @throws RDFParseException
	 * @throws RDFHandlerException
	 */
	private void notifyStatement(Resource sub, IRI pred, Value obj, IRI graph)
			throws RDFParseException, RDFHandlerException {
		Statement statement = createStatement(sub, pred, obj, graph);
		if (rdfHandler != null) {
			try {
				rdfHandler.handleStatement(statement);
			} catch (RDFHandlerException rdfhe) {
				reportFatalError(rdfhe);
				throw rdfhe;
			}
		}
	}

	/**
	 * Consumes spaces until a non space char is detected.
	 * 
	 * @param br
	 *            input stream reader from which consume spaces.
	 * @throws IOException
	 */
	private void consumeSpaces(BufferedReader br) throws IOException {
		char c;
		while (true) {
			mark(br);
			c = readChar(br);
			if (c == ' ' || c == '\r' || c == '\f' || c == '\t') {
				mark(br);
			} else {
				break;
			}
		}
		reset(br);
	}

	/**
	 * Consumes the dot at the end of NQuads line.
	 * 
	 * @param br
	 * @throws IOException
	 */
	private void parseDot(BufferedReader br) throws IOException {
		assertChar(br, '.');
	}

	/**
	 * Parses a URI enclosed within &lt; and &gt; brackets.
	 * 
	 * @param br
	 * @return the parsed URI.
	 * @throws IOException
	 * @throws RDFParseException
	 */
	private IRI parseIRI(BufferedReader br) throws IOException,
			RDFParseException {
		assertChar(br, '<');

		StringBuilder sb = new StringBuilder();
		char c;
		while (true) {
			c = readChar(br);
			if (c != '>') {
				sb.append(c);
			} else {
				break;
			}
		}
		mark(br);

		try {
			// TODO - LOW: used to unescape \\uXXXX unicode chars. Unify with
			// #printEscaped().
			String iriStr = NTriplesUtil.unescapeString(sb.toString());
			IRI iri;
			if (iriStr.charAt(0) == '#') {
				iri = resolveURI(iriStr);
			} else {
				iri = createURI(iriStr);
			}
			return iri;
		} catch (RDFParseException rdfpe) {
			reportFatalError(rdfpe, row, col);
			throw rdfpe;
		}
	}

	/**
	 * Parses a BNode.
	 * 
	 * @param br
	 *            the buffered input stream.
	 * @return the generated bnode.
	 * @throws IOException
	 * @throws RDFParseException
	 */
	private BNode parseBNode(BufferedReader br) throws IOException,
			RDFParseException {
		assertChar(br, '_');
		assertChar(br, ':');

		char c;
		StringBuilder sb = new StringBuilder();
		while (true) {
			c = readChar(br);
			if (c != ' ' && c != '<') {
				sb.append(c);
				mark(br);
			} else {
				break;
			}
		}
		reset(br);

		try {
			return createBNode(sb.toString());
		} catch (RDFParseException rdfpe) {
			reportFatalError(rdfpe, row, col);
			throw rdfpe;
		}
	}

	/**
	 * Parses a literal attribute that can be either the language or the data
	 * type.
	 * 
	 * @param br
	 * @return the literal attribute.
	 * @throws IOException
	 */
	private LiteralAttribute parseLiteralAttribute(BufferedReader br)
			throws IOException {
		char c = readChar(br);
		if (c != '^' && c != '@') {
			reset(br);
			return null;
		}

		boolean isLang = true;
		if (c == '^') {
			isLang = false;
			assertChar(br, '^');
		}

		// Consuming eventual open URI.
		mark(br);
		c = readChar(br);
		if (c != '<') {
			reset(br);
		}

		StringBuilder sb = new StringBuilder();
		while (true) {
			c = readChar(br);
			if (c == '>') {
				mark(br);
				continue;
			}
			if (c != ' ' && c != '<') {
				mark(br);
				sb.append(c);
			} else {
				break;
			}
		}
		reset(br);
		return new LiteralAttribute(isLang, sb.toString());
	}

	/**
	 * Validates and normalize the value of a literal on the basis of the datat
	 * ype handling policy and the associated data type.
	 * 
	 * @param value
	 * @param datatype
	 * @return the normalized data type. It depends on the data type handling
	 *         policy and the specified data type.
	 * @throws RDFParseException
	 */
	private String validateAndNormalizeLiteral(String value, IRI datatype)
			throws RDFParseException {

		if (BasicParserSettings.VERIFY_DATATYPE_VALUES.equals(false) &&  BasicParserSettings.NORMALIZE_DATATYPE_VALUES.equals(false)) {
			return value;
		}

		if (BasicParserSettings.VERIFY_DATATYPE_VALUES.equals(true)) {
			if (!XMLDatatypeUtil.isBuiltInDatatype(datatype)) {
				return value;
			}
			if (!XMLDatatypeUtil.isValidValue(value, datatype)) {
				throw new RDFParseException(String.format(
						"Illegal literal value '%s' with datatype %s", value,
						datatype.stringValue()), row, col);
			}
			return value;
		} else if (BasicParserSettings.NORMALIZE_DATATYPE_VALUES.equals(true)) {
			return XMLDatatypeUtil.normalize(value, datatype);
		} else {
			throw new IllegalArgumentException(String.format(
					"Unsupported datatype handling: %s", BasicParserSettings.DATATYPE_HANDLERS));
		}
	}

	/**
	 * Prints the escaped version of the given char c.
	 * 
	 * @param c
	 *            escaped char.
	 * @param sb
	 *            output string builder.
	 */
	private void printEscaped(char c, StringBuilder sb) {
		if (c == 'b') {
			sb.append('\b');
			return;
		}
		if (c == 'f') {
			sb.append('\f');
			return;
		}
		if (c == 'n') {
			sb.append('\n');
			return;
		}
		if (c == 'r') {
			sb.append('\r');
			return;
		}
		if (c == 't') {
			sb.append('\t');
			return;
		}
	}

	/**
	 * Parses a literal.
	 * 
	 * @param br
	 * @return the parsed literal.
	 * @throws IOException
	 * @throws RDFParseException
	 */
	private Value parseLiteral(BufferedReader br) throws IOException,
			RDFParseException {
		assertChar(br, '"');

		char c;
		boolean escaped = false;
		StringBuilder sb = new StringBuilder();
		while (true) {
			c = readChar(br);
			if (c == '\n' && !escaped) {
				log.warn("Found linebreak in literal, ending this. "
						+ sb.toString());
				break;
			}
			if (c == '\\') {
				if (escaped) {
					escaped = false;
					sb.append(c);
				} else {
					escaped = true;
				}
				continue;
			} else if (c == '"' && !escaped) {
				break;
			}
			if (escaped) {
				if (c == 'u') {
					char unicodeChar = readUnicode(br);
					
					sb.append(unicodeChar);
				} else {
					printEscaped(c, sb);
				}
				escaped = false;
			} else {
				sb.append(c);
			}
		}
		mark(br);

		LiteralAttribute lt = parseLiteralAttribute(br);

		final String value = sb.toString();
		if (lt == null) {
			return createLiteral(value, null, null);
		} else if (lt.isLang) {
			return createLiteral(value, lt.value, null);
		} else {
			IRI literalType = null;
			try {
				literalType = SimpleValueFactory.getInstance().createIRI(lt.value);
			} catch (Exception e) {
				reportError(String.format(
						"Error while parsing literal type '%s'", lt.value),
						row, col, new RioSettingImpl<Boolean>("","",true));
			}
			return createLiteral(
					validateAndNormalizeLiteral(value, literalType), null,
					literalType);
		}
	}

	/**
	 * Parses the subject sequence.
	 * 
	 * @param br
	 * @return the corresponding URI object.
	 * @throws IOException
	 * @throws RDFParseException
	 */
	private Resource parseSubject(BufferedReader br) throws IOException,
			RDFParseException {
		mark(br);
		char c = readChar(br);
		reset(br);
		if (c == '<') {
			return parseIRI(br);
		} else {
			return parseBNode(br);
		}
	}

	/**
	 * Parses the predicate URI.
	 * 
	 * @param br
	 * @return the corresponding URI object.
	 * @throws IOException
	 * @throws RDFParseException
	 */
	private IRI parsePredicate(BufferedReader br) throws IOException,
			RDFParseException {
		return parseIRI(br);
	}

	/**
	 * Parses the the object sequence.
	 * 
	 * @param br
	 * @return the corresponding URI object.
	 * @throws IOException
	 * @throws RDFParseException
	 */
	private Value parseObject(BufferedReader br) throws IOException,
			RDFParseException {
		mark(br);
		char c = readChar(br);
		reset(br);
		if (c == '<') {
			return parseIRI(br);
		} else if (c == '_') {
			return parseBNode(br);
		} else {
			return parseLiteral(br);
		}
	}

	/**
	 * Represents a literal with its attribute value that can be either a
	 * language or a data type.
	 */
	class LiteralAttribute {
		final boolean isLang;
		final String value;

		LiteralAttribute(boolean lang, String value) {
			isLang = lang;
			this.value = value;
		}
	}

	/**
	 * Parses the graph URI.
	 * 
	 * @param br
	 * @return the corresponding URI object.
	 * @throws IOException
	 * @throws RDFParseException
	 */
	private IRI parseGraph(BufferedReader br) throws IOException,
			RDFParseException {
		return parseIRI(br);
	}

	/**
	 * Defines the End Of Stream exception.
	 */
	class EOS extends IOException {
		private static final long serialVersionUID = 1L;
	}

}
