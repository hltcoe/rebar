/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */


package edu.jhu.hlt.rebar.ingest;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import edu.jhu.hlt.concrete.Concrete;

/** Class used to read in a JSON Twitter Place record and convert it to
 * a rebar protobuf object (Concrete.TwitterPlace).  The input JSON record
 * should use the standard twitter API, as defined at:
 *
 * https://dev.twitter.com/docs/platform-objects/places
 */
/*@JsonIgnoreProperties({
	"profile_background_tile", "profile_background_color",
	"profile_background_image_url", "profile_background_image_url_https",
	"profile_image_url", "profile_image_url_https", "profile_link_color",
	"profile_text_color", "profile_sidebar_border_color",
	"profile_sidebar_fill_color", "protected",
	// The following are deprecated in the Twitter API
	"following", "notifications", "id_str",
	// The following are non-standard additions in the mtire data set
	"mitre_gender"})
*/
class TwitterPlaceJsonReader {
	// Public interface
	public static Concrete.TwitterPlace parseJson(String json) throws java.io.IOException {
		if (mapper==null) mapper = new ObjectMapper();
                mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		return mapper.readValue(json, TwitterPlaceJsonReader.class).toProto();
	}

	// Private implementation details
	private static ObjectMapper mapper = null;
	private Concrete.TwitterPlace.Builder builder;
	TwitterPlaceJsonReader() {
		builder = Concrete.TwitterPlace.newBuilder(); }
	Concrete.TwitterPlace toProto() {
		return builder.build(); }

	// Converters: one property for each expected json field.
  	@JsonProperty("place_type") public void setPlaceType(String placeType) {
          if (placeType != null) builder.setPlaceType(placeType); }
	@JsonProperty("country_code") public void setCountryCode(String countryCode) {
          if (countryCode !=null) builder.setCountryCode(countryCode); }
	@JsonProperty("country") public void setCountry(String country) {
          if (country !=null) builder.setCountry(country); }
  	@JsonProperty("full_name") public void setFullName(String fullName) {
          if (fullName !=null) builder.setFullName(fullName); }
  	@JsonProperty("name") public void setName(String name) {
          if (name != null) builder.setName(name); }
	@JsonProperty("id") public void setId(String id) {
          if (id !=null) builder.setId(id); }
  	@JsonProperty("url") public void setUrl(String url) {
          if (url !=null) builder.setUrl(url); }
}
