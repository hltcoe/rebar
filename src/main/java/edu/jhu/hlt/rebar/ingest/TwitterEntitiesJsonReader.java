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

/**
 * Class used to read in a JSON Twitter Entities record and convert it to a rebar protobuf object (Twitter.TwitterEntities). The input JSON record should use
 * the standard twitter API, as defined at:
 * 
 * https://dev.twitter.com/docs/platform-objects/entities
 */
class TwitterEntitiesJsonReader {
  // Public interface
  public static Twitter.TwitterEntities parseJson(String json) throws java.io.IOException {
    if (mapper == null)
      mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    return mapper.readValue(json, TwitterEntitiesJsonReader.class).toProto();
  }

  // Private implementation details
  private static ObjectMapper mapper = null;
  private Twitter.TwitterEntities.Builder builder;

  TwitterEntitiesJsonReader() {
    builder = Twitter.TwitterEntities.newBuilder();
  }

  Twitter.TwitterEntities toProto() {
    return builder.build();
  }

  // Converters: one property for each expected json field.
  @JsonProperty("hashtags")
  public void setHashtags(List<TwitterHashtagJsonReader> hashtags) {
    if (hashtags != null && hashtags.size() > 0) {
      for (TwitterHashtagJsonReader reader : hashtags) {
        builder.addHashtags(reader.toProto());
      }
    }
  }

  @JsonProperty("url")
  public void setUrls(List<TwitterUrlJsonReader> urls) {
    if (urls != null && urls.size() > 0) {
      for (TwitterUrlJsonReader reader : urls) {
        builder.addUrls(reader.toProto());
      }
    }
  }

  @JsonProperty("user_mentions")
  public void setUserMentions(List<TwitterUserMentionJsonReader> userMentions) {
    if (userMentions != null && userMentions.size() > 0) {
      for (TwitterUserMentionJsonReader reader : userMentions) {
        builder.addUserMentions(reader.toProto());
      }
    }
  }
}
