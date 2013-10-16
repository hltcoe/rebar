/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */

package edu.jhu.hlt.rebar.ingest;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.jhu.hlt.concrete.Twitter;

/**
 * Class used to read in a JSON Twitter User record and convert it to a rebar protobuf object (Twitter.TwitterUser). The input JSON record should use the
 * standard twitter API, as defined at:
 * 
 * https://dev.twitter.com/docs/platform-objects/users
 */
@JsonIgnoreProperties({ "profile_background_tile", "profile_background_color", "profile_background_image_url", "profile_background_image_url_https",
    "profile_image_url", "profile_image_url_https", "profile_link_color", "profile_text_color", "profile_sidebar_border_color", "profile_sidebar_fill_color",
    "protected",
    // The following are deprecated in the Twitter API
    "following", "notifications", "id_str",
    // The following are non-standard additions in the mtire data set
    "mitre_gender" })
class TwitterUserJsonReader {
  // Public interface
  public static Twitter.TwitterUser parseJson(String json) throws java.io.IOException {
    if (mapper == null)
      mapper = new ObjectMapper();
    return mapper.readValue(json, TwitterUserJsonReader.class).toProto();
  }

  // Private implementation details
  private static ObjectMapper mapper = null;
  private Twitter.TwitterUser.Builder builder;

  TwitterUserJsonReader() {
    builder = Twitter.TwitterUser.newBuilder();
  }

  Twitter.TwitterUser toProto() {
    return builder.build();
  }

  // Converters: one property for each expected json field.
  @JsonProperty("id")
  public void setId(long id) {
    builder.setId(id);
  }

  @JsonProperty("name")
  public void setName(String name) {
    if (name != null)
      builder.setName(name);
  }

  @JsonProperty("screen_name")
  public void setScreenName(String screenName) {
    if (screenName != null)
      builder.setScreenName(screenName);
  }

  @JsonProperty("lang")
  public void setLang(String lang) {
    if (lang != null)
      builder.setLang(lang);
  }

  @JsonProperty("geo_enabled")
  public void setGeoEnabled(boolean geoEnabled) {
    builder.setGeoEnabled(geoEnabled);
  }

  @JsonProperty("created_at")
  public void setCreatedAt(String createdAt) {
    if (createdAt != null)
      builder.setCreatedAt(createdAt);
  }

  @JsonProperty("friends_count")
  public void setFriendsCount(int friendsCount) {
    builder.setFriendsCount(friendsCount);
  }

  @JsonProperty("statuses_count")
  public void setStatusesCount(int statusesCount) {
    builder.setStatusesCount(statusesCount);
  }

  @JsonProperty("verified")
  public void setVerified(boolean verified) {
    builder.setVerified(verified);
  }

  @JsonProperty("listed_count")
  public void setListedCount(int listedCount) {
    builder.setListedCount(listedCount);
  }

  @JsonProperty("favourites_count")
  public void setFavouritesCount(int favouritesCount) {
    builder.setFavouritesCount(favouritesCount);
  }

  @JsonProperty("followers_count")
  public void setFollowersCount(int followersCount) {
    builder.setFollowersCount(followersCount);
  }

  @JsonProperty("location")
  public void setLocation(String location) {
    if (location != null)
      builder.setLocation(location);
  }

  @JsonProperty("time_zone")
  public void setTimeZone(String timeZone) {
    if (timeZone != null)
      builder.setTimeZone(timeZone);
  }

  @JsonProperty("description")
  public void setDescription(String description) {
    if (description != null)
      builder.setDescription(description);
  }

  @JsonProperty("utc_offset")
  public void setUtcOffset(int utcOffset) {
    builder.setUtcOffset(utcOffset);
  }

  @JsonProperty("url")
  public void setUrl(String url) {
    if (url != null)
      builder.setUrl(url);
  }
}
