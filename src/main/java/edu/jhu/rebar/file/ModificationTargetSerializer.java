/**
 * 
 */
package edu.jhu.rebar.file;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

import edu.jhu.rebar.ProtoIndex.ModificationTarget;

/**
 * @author max
 *
 */
public class ModificationTargetSerializer extends
        Serializer<ModificationTarget> {

    @Override
    public ModificationTarget read(Kryo arg0, Input input,
            Class<ModificationTarget> clazz) {
        return new ModificationTarget(input.readBytes(16));
    }

    @Override
    public void write(Kryo arg0, Output output, ModificationTarget mt) {
        output.write(mt.toBytes());
    }
}
