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
import edu.jhu.hlt.rebar.accumulo.SectionMergingIterator;
import edu.jhu.hlt.rebar.stage.AbstractStageReader;

/**
 * @author max
 *
 */
public class SectionStageReader extends AbstractStageReader {

  /**
   * @param conn
   * @throws RebarException
   */
  public SectionStageReader(String stageName) throws RebarException {
    super(Constants.getConnector(), stageName);
  }

  /**
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
  protected Iterator<Communication> accumuloIterToTIter(Iterator<Entry<Key, Value>> accIter) throws RebarException {
    return new SectionMergingIterator(accIter, stageName);
  }
}
