package org.webdatacommons.webtables.tools.cleaning;

import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import com.google.common.base.Joiner;

/**
 * 
 * The code was mainly copied from the DWTC framework 
 * (https://github.com/JulianEberius/dwtc-extractor & https://github.com/JulianEberius/dwtc-tools)
 * 
 * @author Robert Meusel (robert@informatik.uni-mannheim.de) - Translation to DPEF
 *
 */
public class Analysis {

	private static Joiner joiner = Joiner.on(" ");;

	public static List<String> tokenize(Analyzer analyzer, String keywords) {

		List<String> result = new ArrayList<String>();
		if (keywords.length() == 0)
			return result;
		

		try {
			TokenStream stream = analyzer.tokenStream(null, new StringReader(
					keywords));
			stream.reset();
			while (stream.incrementToken()) {
				result.add(stream.getAttribute(CharTermAttribute.class)
						.toString());
			}
			stream.end();
			stream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public static String analyze(Analyzer analyzer, String keywords) {
		return joiner.join(tokenize(analyzer, keywords));
	}
}