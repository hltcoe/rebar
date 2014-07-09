/*
 * 
 */
package edu.jhu.hlt.rebar.stage.reader;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ScannerBase;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.client.iterators.AutoCloseableIterator;
import edu.jhu.hlt.rebar.client.iterators.TokenizationMergingIterator;

/**
 * @author max
 *
 */
public class TokenizationStageReader extends SentenceStageReader {

  protected final String sentStageDep;
  
  /**
   * 
   * @param stageName
   * @param sectStageDep
   * @param sentStageDep
   * @throws RebarException
   */
  public TokenizationStageReader(String stageName, String sectStageDep, String sentStageDep) throws RebarException {
    this(Constants.getConnector(), stageName, sectStageDep, sentStageDep);
  }

  /**
   * 
   * @param conn
   * @param stageName
   * @param sectStageDep
   * @param sentStageDep
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
  protected AutoCloseableIterator<Communication> accumuloIterToTIter(ScannerBase sc) throws RebarException {
    return new TokenizationMergingIterator(sc, stageName, this.sectStageDep, this.sentStageDep);
  }
}
