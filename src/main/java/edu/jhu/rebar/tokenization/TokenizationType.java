/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */

package edu.jhu.rebar.tokenization;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

import edu.jhu.concrete.Concrete;
import edu.jhu.concrete.Concrete.Tokenization;
import edu.jhu.rebar.util.IdUtil;

/**
 * Enumeration of supported tokenizations.
 * 
 * @author max
 */
public enum TokenizationType {

    PTB {
        @Override
        public Tokenization tokenizeToConcrete(String text, int textStartPosition) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public List<String> tokenize(String text) {
            // TODO Auto-generated method stub
            return null;
        }
    },
    WHITESPACE {
        @Override
        public Tokenization tokenizeToConcrete(String text, int textStartPosition) {
            List<String> tokenList = Arrays.asList(text.split("\\s+"));
            int[] offsets = getOffsets(text, tokenList);
            return generateConcreteTokenization(tokenList, offsets, textStartPosition);
        }
        
        @Override
        public List<String> tokenize(String text) {
            return Arrays.asList(text.split("\\s+"));
        }
    },
    TWITTER_TDT {
        @Override
        public Tokenization tokenizeToConcrete(String text, int textStartPosition) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public List<String> tokenize(String text) {
            // TODO Auto-generated method stub
            return null;
        }
    },
    TWITTER {
        @Override
        public Tokenization tokenizeToConcrete(String text, int textStartPosition) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public List<String> tokenize(String text) {
            // TODO Auto-generated method stub
            return null;
        }
    },
    BASIC {
        @Override
        public Tokenization tokenizeToConcrete(String text, int textStartPosition) {
            // TODO Auto-generated method stub
            return null;
        }

        @Override
        public List<String> tokenize(String text) {
            // TODO Auto-generated method stub
            return null;
        }
    };

    

    
    //
    // Contract methods.
    //
    
    public abstract Tokenization tokenizeToConcrete(String text, int textStartPosition);
    public abstract List<String> tokenize(String text);

    //
    // Patterns to compile.
    //

    // Compile a group of whitespaces into a single whitespace.
    public static final Pattern WHITESPACE_PATTERN = Pattern.compile("\\s+");
    
    //
    // Static methods. 
    //
    /**
     * Return the offsets of tokens in text. 
     * 
     * @param text - text to be used
     * @param tokens
     * @return an integer array of offsets
     */
    public static int[] getOffsets(String text, String[] tokens) {
        int[] r = new int[tokens.length];
        int x = 0;
        for (int i = 0; i < tokens.length; i++) {
            for (int j = x; j < text.length(); j++) {
                if (text.startsWith(tokens[i], j)) {
                    r[i] = j;
                    x = j + tokens[i].length();
                    j = text.length();
                }
            }
        }
        return r;
    }

    /**
     * Wrapper around getOffsets that takes a {@link List} of Strings instead of
     * an array.
     * 
     * @see getOffsets()
     * 
     * @param text
     *            - text that was tokenized
     * @param tokenList
     *            - a {@link List} of tokenized text
     * @return an array of integers that represent offsets
     */
    public static int[] getOffsets(String text, List<String> tokenList) {
        return getOffsets(text, tokenList.toArray(new String[0]));
    }

    public static Concrete.Tokenization generateConcreteTokenization(String[] tokens, int[] offsets, int startPos) {
        return generateConcreteTokenization(Arrays.asList(tokens), offsets, startPos);
    }
    
    public static Concrete.Tokenization generateConcreteTokenization(List<String> tokens, int[] offsets, int startPos) {
        Concrete.Tokenization.Builder tokenizationBuilder = Concrete.Tokenization.newBuilder().setUuid(IdUtil.generateUUID())
                .setKind(Concrete.Tokenization.Kind.TOKEN_LIST);
        // Note: we use token index as token id.
        for (int tokenId = 0; tokenId < tokens.size(); ++tokenId) {
            String token = tokens.get(tokenId);
            int start = startPos + offsets[tokenId];
            int end = start + token.length();
            tokenizationBuilder.addTokenBuilder().setTokenId(tokenId).setText(token)
                    .setTextSpan(Concrete.TextSpan.newBuilder().setStart(start).setEnd(end));
        }

        return tokenizationBuilder.build();
    }
}
