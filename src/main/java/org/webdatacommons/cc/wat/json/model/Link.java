package org.webdatacommons.cc.wat.json.model;

/**
 * 
 * The actual links from the JSON in the WAT file of CC 2013.
 * 
 * @author Robert Meusel
 *
 */
public class Link {

	/**
	 * the anchor text
	 */
	public String text;
	/**
	 * the url
	 */
	public String url;
	/**
	 * the type of the link (e.g. @A/href, @IMG/src, ...)
	 */
	public String path;
}
