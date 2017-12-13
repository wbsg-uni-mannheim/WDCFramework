package org.webdatacommons.cc.wat.json.model;

import com.google.gson.annotations.SerializedName;

/**
 * The root class in the JSON which is used by CC in their WAT files from 2013.
 * Please note this class only contains fields/subclasses which are needed to
 * extract the Links from the JSON. All other features are omitted.
 * 
 * @author Robert Meusel 
 * 
 */
public class Envelope {

	@SerializedName("WARC-Header-Metadata")
	public WarcHeaderMetadata warcHeaderMetadata;

	@SerializedName("Payload-Metadata")
	public PayLoadMetadata payLoadMetadata;

}
