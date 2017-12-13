package org.webdatacommons.structureddata.tablegen;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.any23.vocab.VCard;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.vocabulary.RDF;

public class HCardGenerator extends TableGenerator {
	
	private List<String> tableHeader = Arrays.asList(
			"http://www.w3.org/2006/vcard/ns#email",
			"http://www.w3.org/2006/vcard/ns#family-name",
			"http://www.w3.org/2006/vcard/ns#fn",
			"http://www.w3.org/2006/vcard/ns#given-name",
			"http://www.w3.org/2006/vcard/ns#url");
	
	protected String[] getTableHeader() {
		return tableHeader.toArray(new String[0]);
	}

	protected boolean triggerEntity(Statement stmt) {
		return stmt.getPredicate().equals(RDF.TYPE)
				&& stmt.getObject().equals(VCard.getInstance().VCard);
	}

	protected boolean acceptResult(Map<String, String> entityProperties) {
		return !empty(entityProperties.get(VCard.getInstance().fn.toString()));
	}
	
}
