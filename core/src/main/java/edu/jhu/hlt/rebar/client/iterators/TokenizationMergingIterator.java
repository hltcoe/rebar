/**
 * 
 */
package edu.jhu.hlt.rebar.client.iterators;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.ScannerBase;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.Section;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.concrete.Sentence;
import edu.jhu.hlt.concrete.SentenceSegmentation;
import edu.jhu.hlt.concrete.SentenceSegmentationCollection;
import edu.jhu.hlt.concrete.Tokenization;
import edu.jhu.hlt.concrete.TokenizationCollection;
import edu.jhu.hlt.concrete.UUID;
import edu.jhu.hlt.rebar.RebarException;

/**
 * An {@link Iterator} that will merge {@link TokenizationCollection}s to 
 * {@link Communication} objects.
 * 
 * @author max
 */
public class TokenizationMergingIterator extends SentenceMergingIterator {
  
  protected final String sentDepStage;
  
  /**
   * 
   * @param sc
   * @param stageName
   * @param sectDepStage
   * @param sentDepStage
   */
  public TokenizationMergingIterator(ScannerBase sc, String stageName, String sectDepStage, String sentDepStage) {
    super(sc, stageName, sectDepStage);
    this.sentDepStage = sentDepStage;
  }

  /*
   * (non-Javadoc)
   * @see edu.jhu.hlt.rebar.client.iterators.SectionMergingIterator#next()
   */
  @Override
  public Communication next() {
    try {
      // Get the "row" from the accumulo API.
      Entry<Key, Value> e = this.iter.next();
      Map<Key, Value> rows = WholeRowIterator.decodeRow(e.getKey(), e.getValue());
      // NOTE: ROWS is mutated by below calls
      ThriftRowExtractor ext = new ThriftRowExtractor(rows);
      // NOTE: following structs are mutated heavily by below merge
      Communication root = ext.extractCommunication();
      SectionSegmentation ss = ext.extract(new SectionSegmentation(), this.sectDepStage);
      SentenceSegmentationCollection sColl = ext.extract(new SentenceSegmentationCollection(), this.sentDepStage);
      TokenizationCollection tColl = ext.extract(new TokenizationCollection(), this.stageName);
      this.merge(root, ss, sColl, tColl);
      return root;
    } catch (IOException | RebarException e) {
      throw new RuntimeException(e);
    }  
  }
  
  /**
   * NOTE: mutation to first three parameters.
   * 
   * @param root
   * @param ss
   * @param ssc
   * @throws RebarException
   */
  protected void merge (Communication root, SectionSegmentation ss, SentenceSegmentationCollection ssc, TokenizationCollection tc) throws RebarException {
    this.merge(root, ss, ssc);
    
    // Generate a map of ID --> Sentences
    // so that we can map SentenceSegmentation appropriately
    Map<UUID, Sentence> idToSentenceMap = new HashMap<>();
    for (Section s : ss.getSectionList())
      for (SentenceSegmentation sentSeg : s.getSentenceSegmentation())
        for (Sentence st : sentSeg.getSentenceList())
          idToSentenceMap.put(st.getUuid(), st);
        
    // We have the tokenization collection.
    // Iterate over it and map the Tokenizations
    // to the appropriate sentences.
    for (Tokenization tok : tc.getTokenizationList()) {
      UUID id = tok.getSentenceId();
      // Find the appropriate sentence.
      // If it is not in our map, we need to throw an error - something went wrong.
      // Ideally this would be covered by a validation library.
      if (idToSentenceMap.containsKey(id)) {
        Sentence s = idToSentenceMap.get(id);
        s.addToTokenizationList(tok);
      } else 
        throw new RebarException("A tokenization pointed to a Sentence with id: "
            + id + " , but that id does not map to an existing Sentence from this stage.");
      
    }
  }
}
