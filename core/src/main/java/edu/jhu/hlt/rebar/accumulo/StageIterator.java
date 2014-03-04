/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TException;

import edu.jhu.hlt.asphalt.Stage;

/**
 * @author max
 */
public class StageIterator extends AbstractThriftIterator<Stage> {

  /**
   * 
   */
  public StageIterator(Iterator<Entry<Key, Value>> iter) {
    super(iter);
  }

  @Override
  public Stage next() {
    try {
      Stage c = new Stage();
      deser.deserialize(c, iter.next().getValue().get());
      return c;
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }
}
