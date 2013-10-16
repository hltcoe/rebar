/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */


package edu.jhu.hlt.rebar.ingest;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.jhu.hlt.concrete.Twitter;

/** Class used to read in a JSON Twitter UserMention record and convert it to
 * a rebar protobuf object (Twitter.TwitterEntities.UserMention).  The input JSON record
 * should use the standard twitter API, as defined at:
 *
 * https://dev.twitter.com/docs/platform-objects/entities
 */
class TwitterUserMentionJsonReader {
	// Public interface
	public static Twitter.TwitterEntities.UserMention parseJson(String json) throws java.io.IOException {
		if (mapper==null) mapper = new ObjectMapper();
                mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		return mapper.readValue(json, TwitterUserMentionJsonReader.class).toProto();
	}

	// Private implementation details
	private static ObjectMapper mapper = null;
	private Twitter.TwitterEntities.UserMention.Builder builder;
	TwitterUserMentionJsonReader() {
		builder = Twitter.TwitterEntities.UserMention.newBuilder(); }
	Twitter.TwitterEntities.UserMention toProto() {
		return builder.build(); }

	// Converters: one property for each expected json field.
  	@JsonProperty("indices") public void setIndices(List<Integer> indices) {
          if (indices != null && indices.size() > 0) {
            builder.setStartOffset(indices.get(0));
            builder.setEndOffset(indices.get(1));
          }
        }
	@JsonProperty("screen_name") public void setScreenName(String screenName) {
          if (screenName != null) builder.setScreenName(screenName); }
	@JsonProperty("name") public void setName(String name) {
          if (name != null) builder.setName(name); }
	@JsonProperty("id") public void setId(long id) {
          builder.setId(id); }


}
