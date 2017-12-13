package org.webdatacommons.cc.wat.json.model;

import com.google.gson.annotations.SerializedName;

public class HttpResponseMetadata {
	
	@SerializedName("Headers")
	public Headers headers;
	
	@SerializedName("Response-Message")
	public ResponseMessage responseMessage;
	
	@SerializedName("HTML-Metadata")
	public HtmlMetadata htmlMetadata;

}
