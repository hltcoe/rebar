/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */


package edu.jhu.rebar.ingest;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.jhu.hlt.concrete.Concrete;

/** Class used to read in a JSON Twitter Hashtag record and convert it to
 * a rebar protobuf object (Concrete.TwitterEntities.HashTag).  The input JSON record
 * should use the standard twitter API, as defined at:
 *
 * https://dev.twitter.com/docs/platform-objects/entities
 */
class TwitterHashtagJsonReader {
	// Public interface
	public static Concrete.TwitterEntities.HashTag parseJson(String json) throws java.io.IOException {
		if (mapper==null) mapper = new ObjectMapper();
                mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		return mapper.readValue(json, TwitterHashtagJsonReader.class).toProto();
	}

	// Private implementation details
	private static ObjectMapper mapper = null;
	private Concrete.TwitterEntities.HashTag.Builder builder;
	TwitterHashtagJsonReader() {
		builder = Concrete.TwitterEntities.HashTag.newBuilder(); }
	Concrete.TwitterEntities.HashTag toProto() {
		return builder.build(); }

	// Converters: one property for each expected json field.
  	@JsonProperty("indices") public void setIndices(List<Integer> indices) {
          if (indices != null && indices.size() > 0) {
            builder.setStartOffset(indices.get(0));
            builder.setEndOffset(indices.get(1));
          }
        }
	@JsonProperty("text") public void setText(String text) {
          if (text != null) builder.setText(text); }
}
