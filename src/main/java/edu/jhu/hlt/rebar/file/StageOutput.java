/**
 * 
 */
package edu.jhu.hlt.rebar.file;

import edu.jhu.hlt.concrete.index.ProtoIndex.ModificationTarget;

/**
 * @author max
 * 
 */
public class StageOutput {

  public final int stageId;
  public final ModificationTarget mt;
  public final byte[] mod;

  public StageOutput(int stageId, ModificationTarget mt, byte[] mod) {
    this.stageId = stageId;
    this.mt = mt;
    this.mod = mod;
  }
}
