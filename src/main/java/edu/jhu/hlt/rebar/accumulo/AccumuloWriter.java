/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */

package edu.jhu.hlt.rebar.accumulo;

import java.util.List;
import java.util.Map;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyIterator;
import org.apache.hadoop.io.Text;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;

import edu.jhu.hlt.concrete.Concrete;
import edu.jhu.hlt.concrete.index.ProtoIndex;
import edu.jhu.hlt.concrete.util.ByteUtil;
import edu.jhu.hlt.concrete.util.IdUtil;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Stage;

/** Implementation base class for accumulo "writers". */
/* package-private */class AccumuloWriter {
  private final static byte STAGE_CF_PREFIX = 10;
  private final static Text ROOT_CF = new Text(ByteUtil.fromIntWithPrefix(STAGE_CF_PREFIX, 0));

  private final BatchWriter writer;
  private final String tableName;
  private final AccumuloConnector accumuloConnector;
  private boolean closed;

  public AccumuloWriter(AccumuloConnector accumuloConnector, String tableName) throws RebarException {
    this.accumuloConnector = accumuloConnector;
    this.tableName = tableName;
    this.writer = accumuloConnector.createBatchWriter(tableName);
    this.closed = false;
  }

  /** Return true if the specified row exists. */
  public boolean rowExists(Text rowId) throws RebarException {
    Scanner scanner = accumuloConnector.createScanner(tableName);
    scanner.setRange(new Range(rowId));
    scanner.fetchColumnFamily(ROOT_CF);
    scanner.addScanIterator(new IteratorSetting(1001, "key_only", SortedKeyIterator.class));
    return scanner.iterator().hasNext();
  }

  protected void writeProtoModifications(Stage stage, Text rowId, Map<ProtoIndex.ModificationTarget, byte[]> modifications) throws RebarException {
    // Add the modifications for each target UUID in a cell in the table.
    for (Map.Entry<ProtoIndex.ModificationTarget, byte[]> modification : modifications.entrySet()) {
      Text cf = new Text(ByteUtil.fromIntWithPrefix(STAGE_CF_PREFIX, stage.getStageId()));
      Text cq = new Text(modification.getKey().toBytes());
      write(rowId, cf, cq, new Value(modification.getValue()));
    }
    /** Todo: track/verify dependencies?? */
  }

  protected void writeProtoModifications(Stage stage, Text rowId, ProtoIndex index) throws RebarException {
    Map<ProtoIndex.ModificationTarget, byte[]> modifications = index.getUnsavedModifications();
    if (modifications != null) {
      writeProtoModifications(stage, rowId, modifications);
      index.clearUnsavedModifications();
    }
  }

  protected void write(Text rowId, Text colFamily, Text colQualifier, Value value) throws RebarException {
    Mutation m = new Mutation(rowId);
    m.put(colFamily, colQualifier, value);
    try {
      writer.addMutation(m);
      // if (flush)
      // writer.flush(); // hmm...
    } catch (MutationsRejectedException e) {
      throw new RebarException(e);
    }
  }

  public void flush() throws RebarException {
    if (!closed) {
      try {
        writer.flush();
      } catch (MutationsRejectedException e) {
        throw new RebarException(e);
      }
    }
  }

  public void close() throws RebarException {
    if (!closed) {
      try {
        writer.flush();
        writer.close();
        closed = true;
      } catch (MutationsRejectedException e) {
        throw new RebarException(e);
      }
    }
  }

  /**
   * Merge a single field into a message, based on its UUID, field descriptor, and field value, and return the result. Raise an exception if we don't find
   * anywhere to merge it in.
   * 
   * This method is actually used by Writers to return the newly modified value without having to reread it from the table.
   */
  protected static Message mergeField(Message msg, Concrete.UUID target, FieldDescriptor field, Message fieldValue) throws RebarException {
    Message m = mergeFieldHelper(msg, target, field, fieldValue);
    if (m == null)
      throw new RebarException("UUID not found.");
    return m;
  }

  private static Message mergeFieldHelper(Message msg, Concrete.UUID target, FieldDescriptor field, Message fieldValue) throws RebarException {
    // Check if we have anything to add to this value. If so,
    // then add it and return.
    Concrete.UUID uuid = IdUtil.getUUIDOrNull(msg);
    if ((uuid != null) && target.equals(uuid)) {
      try {
        if (field.isRepeated()) {
          return msg.toBuilder().addRepeatedField(field, fieldValue).build();
        } else {
          return msg.toBuilder().setField(field, fieldValue).build();
        }
      } catch (IllegalArgumentException e) {
        throw new RebarException("Field had unexpected type: " + e);
      }
    }

    // Otherwise, recurse to subfields.
    for (Map.Entry<FieldDescriptor, Object> f : msg.getAllFields().entrySet()) {
      FieldDescriptor fd = f.getKey();
      if (fd.getJavaType() == FieldDescriptor.JavaType.MESSAGE) {
        if (fd.isRepeated()) {
          @SuppressWarnings("unchecked")
          List<Message> children = (List<Message>) f.getValue();
          for (int i = 0; i < children.size(); i++) {
            Message child = children.get(i);
            Message mergedChild = mergeFieldHelper(child, target, field, fieldValue);
            if (mergedChild != null)
              return msg.toBuilder().setRepeatedField(fd, i, mergedChild).build();
          }
        } else {
          Message child = (Message) f.getValue();
          Message mergedChild = mergeFieldHelper(child, target, field, fieldValue);
          if (mergedChild != null)
            return msg.toBuilder().setField(fd, mergedChild).build();
        }
      }
    }
    return null; // no change
  }
}
