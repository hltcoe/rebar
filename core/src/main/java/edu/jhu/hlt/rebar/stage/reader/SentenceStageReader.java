/**
 * 
 */
package edu.jhu.hlt.rebar.stage.reader;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.client.iterators.SentenceMergingIterator;
import edu.jhu.hlt.rebar.stage.AbstractStageReader;

/**
 * @author max
 *
 */
public class SentenceStageReader extends AbstractStageReader {

  protected final String sectStageDep;
  
  /**
   * @param conn
   * @throws RebarException
   */
  public SentenceStageReader(String stageName, String sectStageDep) throws RebarException {
    this(Constants.getConnector(), stageName, sectStageDep);
  }

  /**
   * @param conn
   * @param stageName
   * @throws RebarException
   */
  public SentenceStageReader(Connector conn, String stageName, String sectStageDep) throws RebarException {
    super(conn, stageName);
    this.sectStageDep = sectStageDep;
  }

  /* (non-Javadoc)
   * @see edu.jhu.hlt.rebar.accumulo.AbstractReader#accumuloIterToTIter(java.util.Iterator)
   */
  @Override
  protected Iterator<Communication> accumuloIterToTIter(Iterator<Entry<Key, Value>> accIter) throws RebarException {
    return new SentenceMergingIterator(accIter, stageName, this.sectStageDep);
  }
}
