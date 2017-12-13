package org.webdatacommons.webtables.extraction.model;

//the order within the enum determines the priority:
// the first values are preferred to the last ones in
// case of dominant type determination with equal
// occurrence counts
/**
 * 
 * 
 * The code was mainly copied from the DWT framework 
 * (https://github.com/JulianEberius/dwtc-extractor & https://github.com/JulianEberius/dwtc-tools)
 * 
 * @author Robert Meusel (robert@informatik.uni-mannheim.de) - Translation to DPEF
 *
 */
public enum ContentType {
	FORM,
	HYPERLINK,
	IMAGE,
	ALPHABETICAL,
	DIGIT,
	EMPTY,
	OTHERS
}
		
