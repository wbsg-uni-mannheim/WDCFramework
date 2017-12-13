package org.webdatacommons.webtables.tools.data;

import java.io.Serializable;
/**
 * 
 * The code was mainly copied from the DWTC framework 
 * (https://github.com/JulianEberius/dwtc-extractor & https://github.com/JulianEberius/dwtc-tools)
 * 
 * @author Robert Meusel (robert@informatik.uni-mannheim.de) - Translation to DPEF
 *
 */
public enum HeaderPosition implements Serializable {
	FIRST_ROW,
	FIRST_COLUMN,
	NONE,
	MIXED
}
