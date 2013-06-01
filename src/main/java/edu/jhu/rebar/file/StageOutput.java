/**
 * 
 */
package edu.jhu.rebar.file;

import edu.jhu.rebar.ProtoIndex.ModificationTarget;

/**
 * @author max
 *
 */
public class StageOutput implements Comparable<StageOutput> {

    public final int stageId;
    public final ModificationTarget mt;
    public final byte[] mod;
    
    public StageOutput(int stageId, ModificationTarget mt, byte[] mod) {
        this.stageId = stageId;
        this.mt = mt;
        this.mod = mod;
    }

    @Override
    public int compareTo(StageOutput o) {
        return new Integer(stageId).compareTo(o.stageId);
    }
}
