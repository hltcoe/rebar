/*
 * 
 */
package edu.jhu.hlt.rebar.stage.reader;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.ScannerBase;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.client.iterators.AbstractThriftIterator;
import edu.jhu.hlt.rebar.client.iterators.SentenceMergingIterator;

/**
 * @author max
 *
 */
public class SentenceStageReader extends SectionStageReader {

  protected final String sectStageDep;
  
  /**
   * 
   * @param stageName
   * @param sectStageDep
   * @throws RebarException
   */
  public SentenceStageReader(String stageName, String sectStageDep) throws RebarException {
    this(Constants.getConnector(), stageName, sectStageDep);
  }

  /**
   * 
   * @param conn
   * @param stageName
   * @param sectStageDep
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
  protected AbstractThriftIterator<Communication> accumuloIterToTIter(ScannerBase sc) throws RebarException {
    return new SentenceMergingIterator(sc, stageName, this.sectStageDep);
  }
}
