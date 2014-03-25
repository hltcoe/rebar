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
import edu.jhu.hlt.rebar.client.iterators.TokenizationMergingIterator;

/**
 * @author max
 *
 */
public class TokenizationStageReader extends SentenceStageReader {

  protected final String sentStageDep;
  
  /**
   * @param conn
   * @throws RebarException
   */
  public TokenizationStageReader(String stageName, String sectStageDep, String sentStageDep) throws RebarException {
    this(Constants.getConnector(), stageName, sectStageDep, sentStageDep);
  }

  /**
   * @param conn
   * @param stageName
   * @throws RebarException
   */
  public TokenizationStageReader(Connector conn, String stageName, String sectStageDep, String sentStageDep) throws RebarException {
    super(conn, stageName, sectStageDep);
    this.sentStageDep = sentStageDep;
  }

  /* (non-Javadoc)
   * @see edu.jhu.hlt.rebar.accumulo.AbstractReader#accumuloIterToTIter(java.util.Iterator)
   */
  @Override
  protected Iterator<Communication> accumuloIterToTIter(Iterator<Entry<Key, Value>> accIter) throws RebarException {
    return new TokenizationMergingIterator(accIter, stageName, this.sectStageDep, this.sentStageDep);
  }
}
