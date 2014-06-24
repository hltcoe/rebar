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
import edu.jhu.hlt.rebar.client.iterators.SectionMergingIterator;
import edu.jhu.hlt.rebar.stage.AbstractStageReader;

/**
 * @author max
 *
 */
public class SectionStageReader extends AbstractStageReader {

  /**
   * 
   * @param stageName
   * @throws RebarException
   */
  public SectionStageReader(String stageName) throws RebarException {
    super(Constants.getConnector(), stageName);
  }

  /**
   * 
   * @param conn
   * @param stageName
   * @throws RebarException
   */
  public SectionStageReader(Connector conn, String stageName) throws RebarException {
    super(conn, stageName);
  }

  /* (non-Javadoc)
   * @see edu.jhu.hlt.rebar.accumulo.AbstractReader#accumuloIterToTIter(java.util.Iterator)
   */
  @Override
  protected AbstractThriftIterator<Communication> accumuloIterToTIter(ScannerBase sc) throws RebarException {
    return new SectionMergingIterator(sc, stageName);
  }
}
