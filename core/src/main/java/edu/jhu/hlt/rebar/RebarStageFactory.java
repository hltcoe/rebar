/**
 * 
 */
package edu.jhu.hlt.rebar;

import org.apache.commons.lang.NotImplementedException;
import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

import edu.jhu.hlt.asphalt.Stage;
import edu.jhu.hlt.rebar.stages.SectionStage;

/**
 * @author max
 *
 */
public class RebarStageFactory {

  /**
   * 
   */
  private RebarStageFactory() {
    // TODO Auto-generated constructor stub
  }

  // Suppress warnings: the switch statement should handle any
  // class cast mismatches.
  @SuppressWarnings("unchecked")
  public static <T extends TBase<T, ? extends TFieldIdEnum>> AbstractRebarStage<T> fromStage(Stage s) throws RebarException {
    AbstractRebarStage<T> stage = null;
    switch (s.type) {
    case ENTITIES:
      break;
    case ENTITY_MENTIONS:
      break;
    case LANG_ID:
      break;
    case SECTION:
      stage = (AbstractRebarStage<T>) new SectionStage(s);
      break;
    case SENTENCE:
      break;
    case SITUATIONS:
      break;
    case SITUATION_MENTIONS:
      break;
    case TOKENIZATION:
      break;
    default:
      throw new NotImplementedException("The type: " + s.type.toString() + " is not yet implemented.");
    }
    
    return stage;
  }
}
