/**
 * 
 */
package edu.jhu.hlt.rebar.stages;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.user.WholeRowIterator;
import org.apache.hadoop.io.Text;
import org.apache.thrift.TException;

import edu.jhu.hlt.asphalt.Stage;
import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.rebar.AnnotationException;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Util;
import edu.jhu.hlt.rebar.accumulo.AbstractThriftIterator;
import edu.jhu.hlt.rebar.accumulo.CommunicationReader;
import edu.jhu.hlt.rebar.annotations.AbstractRebarAnnotation;
import edu.jhu.hlt.rebar.annotations.RebarSectionSegmentation;

/**
 * @author max
 *
 */
public class SectionStage extends AbstractStage<SectionSegmentation> {
  
  public SectionStage(Connector conn, Stage stage) throws RebarException {
    super(conn, stage);
  }

  public SectionStage(Stage stage) throws RebarException {
    this(Constants.getConnector(), stage);
  }
  
  private class SectionCommunicationReader extends CommunicationReader {
    public SectionCommunicationReader(Connector conn) throws RebarException {
      super(conn);
    }
    
    protected Iterator<Communication> mergedIterator(String stageName) throws RebarException {
      Range r = new Range("stage:"+stageName);
      Set<Range> ranges = this.scanIndexTableColF(r);
      Iterator<Entry<Key, Value>> eIter = this.batchScanMainTableWholeRowIterator(ranges);
      return this.accumuloIterToTIter(eIter);
    }

    @Override
    protected Iterator<Communication> accumuloIterToTIter(Iterator<Entry<Key, Value>> accIter) throws RebarException {
      return new AbstractThriftIterator<Communication>(accIter) {

        @Override
        public Communication next() {
          try {
            Entry<Key, Value> e = this.iter.next();
            Map<Key, Value> rows = WholeRowIterator.decodeRow(e.getKey(), e.getValue());
            // NOTE: ROWS is mutated by below calls
            Communication root = getCommFromColumnFamily(rows);
            SectionSegmentation ss = getViaColumnFamily(rows);
            root.addToSectionSegmentations(ss);
            return root;
          } catch (IOException | TException | RebarException e) {
            throw new RuntimeException(e);
          }
        }
      };
    }
  }

  public void annotate(SectionSegmentation ss, String docId) throws RebarException, AnnotationException {
    AbstractRebarAnnotation<SectionSegmentation> rss = new RebarSectionSegmentation(ss);
    this.annotate(rss, docId);
  }

  @Override
  public Iterator<Communication> getDocuments() throws RebarException {
    return new SectionCommunicationReader(this.conn).mergedIterator(this.stage.name);
  }
  
  /*
   * TODO:: Refactor below into something more coherent
   */
  @Override
  public SectionSegmentation getViaColumnFamily(Map<Key, Value> decodedRowViaWRI) throws TException, RebarException {
    SectionSegmentation d = null;
    Iterator<Entry<Key, Value>> iter = decodedRowViaWRI.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<Key, Value> entry = iter.next();
      Key k = entry.getKey();
      if (k.compareColumnQualifier(new Text(this.stage.name)) == 0) {
        d = new SectionSegmentation();
        this.deserializer.deserialize(d, entry.getValue().get());
        iter.remove();
        decodedRowViaWRI.remove(k);
      }
    }
    
    if (d == null)
      throw new RebarException("Did not find a section segmentation in this row.");

    return d;
  }
}
