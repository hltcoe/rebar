/**
 * 
 */
package edu.jhu.rebar.util;

import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author max
 *
 */
public class TestTweets {
	
	private static final Logger logger = LoggerFactory
			.getLogger(TestTweets.class);
	
	public static final String TWEET;
	static {
		Scanner sc = new Scanner(TestTweets.class
				.getClassLoader()
				.getResourceAsStream("fake-tweets.txt"), "UTF-8");
		TWEET = sc.nextLine().trim();
		sc.close();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		logger.info("Tweet: " + TWEET);
	}

}
