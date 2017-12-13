package org.webdatacommons.cc.wat.json.model;

import com.google.gson.annotations.SerializedName;

public class WarcHeaderMetadata {
	
	@SerializedName("WARC-Type")
	public String warcType;
	
	@SerializedName("WARC-Target-URI")
	public String url;
	

}
