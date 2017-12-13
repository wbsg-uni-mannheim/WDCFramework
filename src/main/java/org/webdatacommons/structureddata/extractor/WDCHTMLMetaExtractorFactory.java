/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.webdatacommons.structureddata.extractor;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.any23.extractor.ExtractionContext;
import org.apache.any23.extractor.ExtractionException;
import org.apache.any23.extractor.ExtractionParameters;
import org.apache.any23.extractor.ExtractionResult;
import org.apache.any23.extractor.Extractor;
import org.apache.any23.extractor.ExtractorDescription;
import org.apache.any23.extractor.SimpleExtractorFactory;
import org.apache.any23.extractor.html.DomUtils;
import org.apache.any23.rdf.PopularPrefixes;
import org.apache.any23.rdf.Prefixes;
import org.apache.any23.rdf.RDFUtils;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.ValueFactory;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.w3c.dom.Document;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;

/**
 * This extractor is a copy which is based on the
 * {@link org.apache.any23.extractor.html.HTMLMetaExtractor} by any23 but uses
 * another Namespace "http://webdatacommons.org/meta/".
 * 
 * @author Davide Palmisano ( dpalmisano@gmail.com )
 * @author Robert Meusel (only changes according to namespace)
 */
class WDCHTMLMetaExtractor implements Extractor.TagSoupDOMExtractor {

	private static String NS = "http://webdatacommons.org/meta/";

	public static final String NAME = "html-head-meta";

	private IRI profile;

	private Map<String, IRI> prefixes = new HashMap<String, IRI>();

	private String documentLang;

	ValueFactory vf = SimpleValueFactory.getInstance();

	/**
	 * {@inheritDoc}
	 */
	public void run(ExtractionParameters extractionParameters,
			ExtractionContext extractionContext, Document in,
			ExtractionResult out) throws IOException, ExtractionException {
		profile = extractProfile(in);
		documentLang = getDocumentLanguage(in);
		extractLinkDefinedPrefixes(in);

		String baseProfile = NS;
		if (profile != null) {
			baseProfile = profile.toString();
		}

		final IRI documentIRI = extractionContext.getDocumentIRI();
		Set<Meta> metas = extractMetaElement(in, baseProfile);
		for (Meta meta : metas) {
			String lang = documentLang;
			if (meta.getLang() != null) {
				lang = meta.getLang();
			}
			out.writeTriple(documentIRI, meta.name, 
					vf.createLiteral(meta.getContent(), lang));
		}
	}

	/**
	 * Returns the {@link Document} language if declared, <code>null</code>
	 * otherwise.
	 * 
	 * @param in
	 *            a instance of {@link Document}.
	 * @return the language declared, could be <code>null</code>.
	 */
	private String getDocumentLanguage(Document in) {
		String lang = DomUtils.find(in, "string(/HTML/@lang)");
		if (lang.equals("")) {
			return null;
		}
		return lang;
	}

	private IRI extractProfile(Document in) {
		String profile = DomUtils.find(in, "string(/HTML/@profile)");
		if (profile.equals("")) {
			return null;
		}
		return vf.createIRI(profile);
	}

	/**
	 * It extracts prefixes defined in the <i>LINK</i> meta tags.
	 * 
	 * @param in
	 */
	private void extractLinkDefinedPrefixes(Document in) {
		List<Node> linkNodes = DomUtils.findAll(in, "/HTML/HEAD/LINK");
		for (Node linkNode : linkNodes) {
			NamedNodeMap attributes = linkNode.getAttributes();
			String rel = attributes.getNamedItem("rel").getTextContent();
			String href = attributes.getNamedItem("href").getTextContent();
			if (rel != null && href != null && RDFUtils.isAbsoluteIRI(href)) {				
				prefixes.put(rel, vf.createIRI(href));
			}
		}
	}

	private Set<Meta> extractMetaElement(Document in, String baseProfile) {
		List<Node> metaNodes = DomUtils.findAll(in, "/HTML/HEAD/META");
		Set<Meta> result = new HashSet<Meta>();
		for (Node metaNode : metaNodes) {
			NamedNodeMap attributes = metaNode.getAttributes();
			Node nameAttribute = attributes.getNamedItem("name");
			Node contentAttribute = attributes.getNamedItem("content");
			if (nameAttribute == null || contentAttribute == null) {
				continue;
			}
			String name = nameAttribute.getTextContent();
			String content = contentAttribute.getTextContent();
			String xpath = DomUtils.getXPathForNode(metaNode);
			IRI nameAsIRI = getPrefixIfExists(name);
			if (nameAsIRI == null) {
				nameAsIRI =  vf.createIRI(baseProfile + name);
			}
			Meta meta = new Meta(xpath, nameAsIRI, content);
			result.add(meta);
		}
		return result;
	}

	private IRI getPrefixIfExists(String name) {
		String[] split = name.split("\\.");
		if (split.length == 2 && prefixes.containsKey(split[0])) {
			return vf.createIRI(prefixes.get(split[0]) + split[1]);
		}
		return null;
	}

	public ExtractorDescription getDescription() {
        return WDCHTMLMetaExtractorFactory.getDescriptionInstance();
	}

	private class Meta {

		private String xpath;

		private IRI name;

		private String lang;

		private String content;

		public Meta(String xpath, IRI name, String content) {
			this.xpath = xpath;
			this.name = name;
			this.content = content;
		}

		public Meta(String xpath, IRI name, String content, String lang) {
			this(xpath, name, content);
			this.lang = lang;
		}

		public IRI getName() {
			return name;
		}

		public void setName(IRI name) {
			this.name = name;
		}

		public String getLang() {
			return lang;
		}

		public void setLang(String lang) {
			this.lang = lang;
		}

		public String getContent() {
			return content;
		}

		public void setContent(String content) {
			this.content = content;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o)
				return true;
			if (o == null || getClass() != o.getClass())
				return false;

			Meta meta = (Meta) o;

			if (xpath != null ? !xpath.equals(meta.xpath) : meta.xpath != null)
				return false;

			return true;
		}

		@Override
		public int hashCode() {
			return xpath != null ? xpath.hashCode() : 0;
		}
	}

}

public class WDCHTMLMetaExtractorFactory extends SimpleExtractorFactory<WDCHTMLMetaExtractor>{

    public static final String NAME = "html-head-meta";

    public static final Prefixes PREFIXES = PopularPrefixes.createSubset("sindice");

    private static final ExtractorDescription descriptionInstance = new IgnorantHCardExtractorFactory();


    public WDCHTMLMetaExtractorFactory() {
        super(
                WDCHTMLMetaExtractorFactory.NAME,
                WDCHTMLMetaExtractorFactory.PREFIXES,
                Arrays.asList("text/html;q=0.02","application/xhtml+xml;q=0.02"),
                "example-meta.html");
    }

    @Override
    public WDCHTMLMetaExtractor createExtractor() {
        return new WDCHTMLMetaExtractor();
    }

    public static ExtractorDescription getDescriptionInstance() {
        return descriptionInstance;
    }
}
