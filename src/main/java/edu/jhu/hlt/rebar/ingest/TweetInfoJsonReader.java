/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */

package edu.jhu.hlt.rebar.ingest;

import java.io.IOException;
import java.util.LinkedHashMap;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.jhu.hlt.concrete.Twitter;

/**
 * Class used to read in a JSON Tweet record and convert it to a rebar protobuf
 * object (Concrete.TweetInfo). The input JSON record should use the standard
 * twitter API, as defined at:
 * 
 * https://dev.twitter.com/docs/platform-objects/tweets
 */
public class TweetInfoJsonReader {
    // Uncomment this if you want to print out every unknown property name
    // (hint: you don't)
    /*
     * @JsonAnySetter public void handleUnknown(String key, Object value){
     * System.out.println("Unknown value: "+key); }
     */
    // Public interface
    public static Twitter.TweetInfo parseJson(String json) throws JsonParseException, JsonMappingException, IOException {
        if (mapper == null)
            mapper = new ObjectMapper();
        mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        return mapper.readValue(json, TweetInfoJsonReader.class).toProto();
    }

    // Private implementation details
    private static ObjectMapper mapper = null;
    private Twitter.TweetInfo.Builder builder;

    TweetInfoJsonReader() {
        builder = Twitter.TweetInfo.newBuilder();
    }

    Twitter.TweetInfo toProto() {
        return builder.build();
    }

    // Converters: one property for each expected json field.
    @JsonProperty("id")
    public void setId(long id) {
        builder.setId(id);
    }

    @JsonProperty("text")
    public void setText(String text) {
        if (text != null)
            builder.setText(text);
    }

    @JsonProperty("created_at")
    public void setCreatedAt(String createdAt) {
        if (createdAt != null)
            builder.setCreatedAt(createdAt);
    }

    @JsonProperty("user")
    public void setUser(TwitterUserJsonReader user) {
        builder.setUser(user.toProto());
    }

    @JsonProperty("truncated")
    public void setTruncated(boolean truncated) {
        builder.setTruncated(truncated);
    }

    @JsonProperty("source")
    public void setSource(String source) {
        if (source != null)
            builder.setSource(source);
    }

    @JsonProperty("favorited")
    public void setFavorited(boolean favorited) {
        builder.setFavorited(favorited);
    }

    @JsonProperty("retweeted_status")
    public void setRetweeted(LinkedHashMap<String, Object> retweeted) {
        if (retweeted != null && retweeted.size() > 0)
            builder.setRetweeted(true);
    }

    // @JsonProperty("retweet_count") public void setRetweetCount(int
    // retweetCount) {
    // builder.setRetweetCount(retweetInt); }
    @JsonProperty("in_reply_to_screen_name")
    public void setInReplyToScreenName(String inReplyToScreenName) {
        if (inReplyToScreenName != null)
            builder.setInReplyToScreenName(inReplyToScreenName);
    }

    @JsonProperty("in_reply_to_status_id")
    public void setInReplyToStatusId(long inReplyToStatusId) {
        builder.setInReplyToStatusId(inReplyToStatusId);
    }

    @JsonProperty("in_reply_to_user_id")
    public void setInReplyToUserId(long inReplyToUserId) {
        builder.setInReplyToUserId(inReplyToUserId);
    }

    @JsonProperty("place")
    public void setPlace(TwitterPlaceJsonReader place) {
        if (place != null)
            builder.setPlace(place.toProto());
    }

    @JsonProperty("coordinates")
    public void setCoordinates(TwitterCoordinatesJsonReader coordinates) {
        if (coordinates != null)
            builder.setCoordinates(coordinates.toProto());
    }

    @JsonProperty("entities")
    public void setEntities(TwitterEntitiesJsonReader entities) {
        if (entities != null)
            builder.setEntities(entities.toProto());
    }

}
