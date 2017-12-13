package org.webdatacommons.cc.wat.json;

import org.webdatacommons.cc.wat.json.model.JsonData;

import com.google.gson.Gson;

/**
 * Reader to transform textual representation of JSON into the {@link JsonData} object.
 * @author Robert Meusel
 *
 */
public class WatJsonReader {

	/**
	 * Transforms a pure {@link String} representation of the JSON into a {@link JsonData} object.
	 * @param The JSON as {@link String}
	 * @return the {@link JsonData} object from the {@link String} representation
	 */
	public static JsonData read(String json) {
		Gson gson = new Gson();		
		return gson.fromJson(json, JsonData.class);
	}
}
