package edu.jhu.rebar.tokenization;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.concrete.Concrete.Token;
import edu.jhu.concrete.Concrete.Tokenization;

public class TokenizationTypeTest {
    
    private static final Logger logger = Logger.getLogger(TokenizationTypeTest.class);

    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void testTokenizeToConcrete() {
        String text = "hello this is a test";
        int expectedTokenCount = 5;
        Tokenization ct = TokenizationType.WHITESPACE.tokenizeToConcrete(text, 0);
        List<Token> tokenList = ct.getTokenList();
        assertEquals(expectedTokenCount, tokenList.size());
        for (Token t : tokenList)
            logger.info("Got token: " + t.getTokenId() + " with text: " + t.getText());
    }

    @Test
    public void testTokenize() {
        String text = "hello this is a test";
        int expectedTokenCount = 5;
        List<String> tokenList = TokenizationType.WHITESPACE.tokenize(text);
        assertEquals(expectedTokenCount, tokenList.size());
        //fail("Not yet implemented");
    }

}
