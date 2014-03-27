/**
 * 
 */
package edu.jhu.hlt.rebar.client.iterators;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.concrete.SentenceSegmentationCollection;
import edu.jhu.hlt.concrete.SituationMentionSet;
import edu.jhu.hlt.concrete.TokenizationCollection;
import edu.jhu.hlt.rebar.RebarException;

/**
 * An {@link Iterator} that will merge {@link SituationMentionSet}s to 
 * {@link Communication} objects.
 * 
 * @author max
 */
public class SituationMentionMergingIterator extends TokenizationMergingIterator {
  
  protected final String tokenizationDepStage;
  
  /**
   * 
   * @param iter
   * @param stageName
   * @param sectDepStage
   * @param sentDepStage
   */
  public SituationMentionMergingIterator(Iterator<Entry<Key, Value>> iter, String stageName, String sectDepStage, 
      String sentDepStage, String tokenizationDepStage) {
    super(iter, stageName, sectDepStage, sentDepStage);
    this.tokenizationDepStage = tokenizationDepStage;
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
      TokenizationCollection tColl = ext.extract(new TokenizationCollection(), this.tokenizationDepStage);
      SituationMentionSet ms = ext.extract(new SituationMentionSet(), this.stageName);
      
      this.merge(root, ss, sColl, tColl);
      this.merge(root, ms);
      return root;
    } catch (IOException | RebarException e) {
      throw new RuntimeException(e);
    }  
  }
  
  /**
   * NOTE: mutation to root.
   * 
   * @param root
   * @param ss
   * @param ssc
   * @throws RebarException
   */
  protected void merge (Communication root, SituationMentionSet ms) throws RebarException {
    root.addToSituationMentionSets(ms);
  }
}
