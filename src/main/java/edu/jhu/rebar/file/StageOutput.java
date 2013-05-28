/**
 * 
 */
package edu.jhu.rebar.file;

import edu.jhu.rebar.ProtoIndex.ModificationTarget;

/**
 * @author max
 *
 */
public class StageOutput {

    public final ModificationTarget mt;
    public final byte[] mod;
    
    public StageOutput(ModificationTarget mt, byte[] mod) {
        this.mt = mt;
        this.mod = mod;
    }
}
