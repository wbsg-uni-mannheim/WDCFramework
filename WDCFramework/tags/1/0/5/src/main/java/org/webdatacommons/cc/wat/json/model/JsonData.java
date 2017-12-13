package org.webdatacommons.cc.wat.json.model;

import com.google.gson.annotations.SerializedName;

/**
 * Wrapper class for JSON objects from CC WAT files.
 * @author Robert Meusel
 *
 */
public class JsonData {
	
	@SerializedName("Envelope")
	public Envelope envelope;

}
