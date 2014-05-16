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
import edu.jhu.hlt.concrete.SentenceSegmentation;
import edu.jhu.hlt.concrete.SentenceSegmentationCollection;
import edu.jhu.hlt.rebar.RebarException;

/**
 * An {@link Iterator} that will merge {@link SentenceSegmentationCollection}s to 
 * {@link Communication} objects.
 * 
 * @author max
 */
public class SentenceMergingIterator extends SectionMergingIterator {
  
  protected final String sectDepStage;
  
  /**
   * @param iter
   * @param stageName
   */
  public SentenceMergingIterator(ScannerBase sc, String stageName, String sectDepStage) {
    super(sc, stageName);
    this.sectDepStage = sectDepStage;
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
      // NOTE: we will mutate "root" heavily
      Communication root = ext.extractCommunication();
      SectionSegmentation ss = ext.extract(new SectionSegmentation(), this.sectDepStage);
      SentenceSegmentationCollection sColl = ext.extract(new SentenceSegmentationCollection(), stageName);
      this.merge(root, ss, sColl);
      return root;
    } catch (IOException | RebarException e) {
      throw new RuntimeException(e);
    }
  }
  
  /**
   * NOTE: mutation to first two parameters.
   * 
   * @param root
   * @param ss
   * @param ssc
   * @throws RebarException
   */
  protected void merge (Communication root, SectionSegmentation ss, SentenceSegmentationCollection ssc) throws RebarException {
    this.merge(root, ss);
    
    // Generate a map of ID --> Sections
    // so that we can map SentenceSegmentation appropriately
    Map<String, Section> idToSectionMap = new HashMap<>();
    for (Section s : ss.getSectionList())
      idToSectionMap.put(s.uuid, s);

    // We have the section segmentation collection.
    // Iterate over it and map the SentenceSegmentations
    // to the appropriate sections.
    for (SentenceSegmentation sentSeg : ssc.getSentSegList()) {
      String id = sentSeg.getSectionId();
      // Find the appropriate section.
      // If it is not in our map, we need to throw an error - something went wrong.
      // Ideally this would be covered by a validation library.
      if (idToSectionMap.containsKey(id)) {
        Section s = idToSectionMap.get(id);
        s.addToSentenceSegmentation(sentSeg);
      } else 
        throw new RebarException("A sentence segmentation pointed to a Section with id: "
            + id + " , but that id does not map to an existing Section from this stage.");
      
    }
  }
}
