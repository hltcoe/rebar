/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */


// Copyright 2010-2012 Benjamin Van Durme. All rights reserved.
// This software is released under the 2-clause BSD license.
// See jerboa/LICENSE, or http://cs.jhu.edu/~vandurme/jerboa/LICENSE

package edu.jhu.rebar.tokenization;

import java.io.IOException;
import java.util.Vector;

/**
 *
 * Implementation of various tokenization schemes.
 *
 * @author Benjamin Van Durme 
 */ 

public class Tokenizer {

  public static String[] tokenizeTweetPetrovic (String text) {
    int length = text.length();
    int state = 0;
    String token = "";
    char c;
    int cType;
    boolean update = false;
    Vector<String> content = new Vector<String>();

    // Sasa Petrovic's tokenization scheme.
    //
    // My (vandurme) one change was to add UPPERCASE_LETTER as another
    // option alongside LOWER_CASE_LETTER
    for (int i = 0; i < length; i++) {
      c = text.charAt(i);
      cType = Character.getType(c);
      //System.out.print(" " + cType + " ");

      //System.out.println(token);
                        
      switch (state) {
      case 0 : // Start state
        //System.out.println("[" + token + "]");
        token = "";
        if (cType == Character.SPACE_SEPARATOR) break;
        // link
        // Characters matched out of order to fail
        // early when not a link.
        else if ((c == 'h') &&
                 (i + 6 < length) &&
                 (text.charAt(i+4) == ':') &&
                 (text.charAt(i+5) == '/')) {
          token += c;
          state = 4; break;
        }
        // normal
        else if ((cType == Character.LOWERCASE_LETTER) ||
                 (cType == Character.UPPERCASE_LETTER) ||
                 (cType == Character.DECIMAL_DIGIT_NUMBER)) {
          token += c;
          state = 1; break;
        }
        // @reply
        else if (c == '@') {
          token += c;
          state = 2; break;
        }
        // #topic
        else if (c == '#') {
          token += c;
          state = 3; break;
        }
        else break;
      case 1 : // Normal
        //System.out.println("s1");
        if ((cType == Character.LOWERCASE_LETTER) ||
            (cType == Character.UPPERCASE_LETTER) ||
            (cType == Character.DECIMAL_DIGIT_NUMBER)) {
          token += c;
          break;
        }
        else {
          update = true;
          state = 0; break;
        }
      case 2 : // @reply
        //System.out.println("s2");
        // Author names may have underscores,
        // which we don't want to split on here
        if ((cType == Character.LOWERCASE_LETTER) ||
            (cType == Character.UPPERCASE_LETTER) ||
            (cType == Character.DECIMAL_DIGIT_NUMBER) ||
            (c == '_')) {
          token += c;
          break;
        }
        else {
          update = true;
          state = 0; break;
        }
      case 3 : // #topic
        //System.out.println("s3");
        // This could just be state 1, with special care
        // taken in state 0 when the topic is first
        // recognized, but I'm staying aligned to Sasa's
        // code
        if ((cType == Character.LOWERCASE_LETTER) ||
            (cType == Character.UPPERCASE_LETTER) ||
            (cType == Character.DECIMAL_DIGIT_NUMBER)) {
          token += c;
          break;
        }
        else {
          update = true;
          state = 0; break;
        }
      case 4 : // link
        //System.out.println("s4");
        if ((cType == Character.SPACE_SEPARATOR) ||
            (c == '[')) {
          //if ((c == ' ') || (c == '[')) {
          update = true;
          state = 0; break;
        } else {
          token += c;
          break;
        }
      }
	    
      if (update || ((i == (length-1)) && (!token.equals("")))) {
        content.add(token);
        update = false;
      }
    }
    return (String[]) content.toArray(new String[0]);
  }

  

  public static String[] tokenize (String text, TokenizationType type) throws IOException {
    switch (type) {
    case PTB:
      return Rewriter.PTB.rewrite(text).split("\\s+");
    case BASIC:
      return Rewriter.BASIC.rewrite(text).split("\\s+");
    case WHITESPACE:
      return text.split("\\s+");
    case TWITTER_TDT:
      return tokenizeTweetPetrovic(text);
    case TWITTER:
      return TwitterTokenizer.tokenizeTweet(text);
    default: return null;
    }
  }

}
