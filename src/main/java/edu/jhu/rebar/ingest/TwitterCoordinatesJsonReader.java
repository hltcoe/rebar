/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */


package edu.jhu.rebar.ingest;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.jhu.concrete.Concrete;

/** Class used to read in a JSON Twitter Coordinate record and convert it to
 * a rebar protobuf object (Concrete.TwitterCoordinates).  The input JSON record
 * should use the standard twitter API, as defined at:
 *
 * https://dev.twitter.com/docs/platform-objects/tweets#obj-coordinates
 */

class TwitterCoordinatesJsonReader {
	// Public interface
	public static Concrete.TwitterCoordinates parseJson(String json) throws java.io.IOException {
		if (mapper==null) mapper = new ObjectMapper();
		return mapper.readValue(json, TwitterCoordinatesJsonReader.class).toProto();
	}

	// Private implementation details
	private static ObjectMapper mapper = null;
	private Concrete.TwitterCoordinates.Builder builder;
	TwitterCoordinatesJsonReader() {
		builder = Concrete.TwitterCoordinates.newBuilder(); }
	Concrete.TwitterCoordinates toProto() {
		return builder.build(); }
  	// Converters: one property for each expected json field.
	@JsonProperty("type") public void setType(String type) {
          builder.setType(type); }
  	@JsonProperty("coordinates") public void setCoordinates(List<Double> coordinates) {
          if (coordinates != null && coordinates.size() == 2) {
            Concrete.TwitterLatLong.Builder builder = Concrete.TwitterLatLong.newBuilder();
            builder.setLongitude(coordinates.get(0));
            builder.setLatitude(coordinates.get(1));
            this.builder.setCoordinates(builder.build());
          }
        }
}
