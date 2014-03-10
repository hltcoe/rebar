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
import edu.jhu.hlt.concrete.Section;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.rebar.AnnotationException;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Util;
import edu.jhu.hlt.rebar.accumulo.AbstractReader;
import edu.jhu.hlt.rebar.accumulo.AbstractThriftIterator;
import edu.jhu.hlt.rebar.annotations.AbstractRebarAnnotation;

/**
 * @author max
 *
 */
public class SectionStage extends AbstractStage<SectionSegmentation> {
  
  private class SectionCommunicationReader extends AbstractReader<Communication> {

    public SectionCommunicationReader() throws RebarException {
      this(Constants.getConnector());
    }
    
    public SectionCommunicationReader(Connector conn) throws RebarException {
      super(conn, Constants.DOCUMENT_TABLE_NAME, Constants.DOCUMENT_IDX_TABLE);
    }
    
    public Iterator<Communication> mergedIterator(String stageName) throws RebarException {
      try {
        Range r = new Range("stage:"+stageName);
        Set<Range> ranges = this.scanIndexTableColF(r);
        Iterator<Entry<Key, Value>> eIter = this.batchScanMainTableWholeRowIterator(ranges);
        return this.accumuloIterToTIter(eIter);
      } finally {
        
      }
    }

    @Override
    protected Iterator<Communication> accumuloIterToTIter(final Iterator<Entry<Key, Value>> accIter) throws RebarException {
      return new AbstractThriftIterator<Communication>(accIter) {

        @Override
        public Communication next() {
          try {
            Entry<Key, Value> e = this.iter.next();
            Map<Key, Value> rows = WholeRowIterator.decodeRow(e.getKey(), e.getValue());
            // NOTE: ROWS is mutated by call to getRoot
            Communication root = getCommFromColumnFamily(rows);
            SectionSegmentation ss = getSectionSegFromColumnFamily(rows);
            root.addToSectionSegmentations(ss);
            return root;
          } catch (IOException | TException | RebarException e) {
            throw new RuntimeException(e);
          }
        }
      };
    }
  }
  
  public SectionStage(Connector conn, Stage stage) throws RebarException {
    super(conn, stage);
  }

  public SectionStage(Stage stage) throws RebarException {
    this(Constants.getConnector(), stage);
  }

  public void annotate(SectionSegmentation ss, String docId) throws RebarException, AnnotationException {
    try {
      Communication c = this.reader.get(docId);
      AbstractRebarAnnotation<SectionSegmentation> rss = new AbstractRebarAnnotation<SectionSegmentation>(ss) {

        @Override
        public boolean validate(Communication c) throws RebarException {
          boolean valid = 
              this.annotation.metadata != null
              && Util.isValidUUIDString(this.annotation.uuid)
              && this.annotation.isSetSectionList();
          Iterator<Section> sects = this.annotation.getSectionListIterator();
          while (valid && sects.hasNext()) {
            Section s = sects.next();
            valid = 
                Util.isValidUUIDString(s.uuid);
          }
          
          return valid;
        }
      };
      
      if (rss.validate(c)) {
        Mutation m = new Mutation(c.uuid);
        m.put("annotations", this.stage.name, new Value(this.serializer.serialize(ss)));

        this.idxBw.addMutation(Util.generateEmptyValueMutation("stage:"+this.stage.name, c.uuid, ""));
        this.bw.addMutation(m);
      } else {
        throw new AnnotationException("Your SectionSegmentation object was invalid.");
      }
    } catch (TException | MutationsRejectedException e) {
      throw new RebarException(e);
    }
  }

  @Override
  public Iterator<Communication> getDocuments() throws RebarException {
    return new SectionCommunicationReader(this.conn).mergedIterator(this.stage.name);
  }
  
  /*
   * TODO:: Refactor below into something more coherent
   */
  /**
   * 
   * @param decodedRowViaWRI
   * @return
   * @throws TException
   * @throws RebarException
   */
  public Communication getCommFromColumnFamily(Map<Key, Value> decodedRowViaWRI) throws TException, RebarException {
    Communication d = null;
    Iterator<Entry<Key, Value>> iter = decodedRowViaWRI.entrySet().iterator();
    while (iter.hasNext()) {
      Entry<Key, Value> entry = iter.next();
      Key k = entry.getKey();
      if (k.compareColumnFamily(new Text(Constants.DOCUMENT_COLF)) == 0) {
        d = new Communication();
        this.deserializer.deserialize(d, entry.getValue().get());
        iter.remove();
        decodedRowViaWRI.remove(k);
      }
    }
    
    if (d == null)
      throw new RebarException("Did not find a root communication in this row.");

    return d;
  }
  
  public SectionSegmentation getSectionSegFromColumnFamily(Map<Key, Value> decodedRowViaWRI) throws TException, RebarException {
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
