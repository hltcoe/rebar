/**
 * 
 */
package edu.jhu.rebar.tokenization;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

/**
 * Wrapper for mass SimpleImmutableEntry<Pattern, String> in TwitterTokenizer.
 * 
 * @author max
 *
 */
public class PatternStringTuple {
    private final Pattern pattern;
    private final String entry;
    
    /**
     * Default constructor.
     * 
     * @param pattern
     * @param entry
     */
    public PatternStringTuple(Pattern pattern, String entry) {
        this.pattern = pattern;
        this.entry = entry;
    }
    
    /**
     * Compile the first parameter into a {@link java.util.regex.Pattern} object.
     * 
     * @param pattern
     * @param entry
     */
    public PatternStringTuple(String pattern, String entry) {
        this.pattern = Pattern.compile(pattern);
        this.entry = entry;
    }
    
    /**
     * Given two string arrays of equal length, create a {@link java.util.List} of 
     * {@link PatternStringTuple} objects that correspond to the indices of each array
     * (e.g., tupleList.get(0) contains patterns[0] and entries[0]).
     * 
     * @param patterns 
     * @param entries
     * @return
     */
    public static List<PatternStringTuple> mapPatterns(String[] patterns, String[] entries) {
        final List<PatternStringTuple> tupleList = new ArrayList<>();
        
        if (patterns.length != entries.length) {
            throw new IllegalArgumentException("Length of patterns array [" + patterns.length + "] was not equal to " +
                    "length of entries array [" + entries.length + "]");
        }
        
        for (int i = 0; i < patterns.length; i++) {
            tupleList.add(new PatternStringTuple(Pattern.compile(patterns[0]), entries[0]));
        }
        
        return tupleList;
    }

    /**
     * @return the pattern
     */
    public Pattern getPattern() {
        return pattern;
    }

    /**
     * @return the entry
     */
    public String getEntry() {
        return entry;
    }
}