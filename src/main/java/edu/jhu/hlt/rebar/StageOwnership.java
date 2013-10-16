/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */

package edu.jhu.hlt.rebar;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import com.google.protobuf.Descriptors.FieldDescriptor;

/**
 * A class that records what stage created each portion of a protobuf object. This is intended primarily for display purposes, when users are looking at the
 * contents of a corpus or graph, and wish to see which pieces were generated by which stages.
 * */
public class StageOwnership extends HashMap<StageOwnership.FieldValuePointer, Stage> {
  private boolean frozen;

  public StageOwnership() {
    frozen = false;
  }

  public void freeze() {
    frozen = true;
  }

  // ======================================================================
  // Immutable -- disable mutating operations after freeze() is called.
  // ======================================================================

  @Override
  public void clear() {
    if (frozen)
      throw new UnsupportedOperationException("Immutable map");
    else
      super.clear();
  }

  @Override
  public Stage put(StageOwnership.FieldValuePointer key, Stage value) {
    if (frozen)
      throw new UnsupportedOperationException("Immutable map");
    else
      return super.put(key, value);
  }

  @Override
  public void putAll(Map<? extends StageOwnership.FieldValuePointer, ? extends Stage> m) {
    if (frozen)
      throw new UnsupportedOperationException("Immutable map");
    else
      super.putAll(m);
  }

  public Stage remove(StageOwnership.FieldValuePointer key) {
    if (frozen)
      throw new UnsupportedOperationException("Immutable map");
    else
      return super.remove(key);
  }

  @Override
  public Set<Map.Entry<StageOwnership.FieldValuePointer, Stage>> entrySet() {
    if (frozen)
      return Collections.unmodifiableSet(super.entrySet());
    else
      return super.entrySet();
  }

  @Override
  public Collection<Stage> values() {
    if (frozen)
      return Collections.unmodifiableCollection(super.values());
    else
      return super.values();
  }

  // ======================================================================
  // Field Value Pointer (key for this map)
  // ======================================================================

  /**
   * A pointer to a single field value in a complex protobuf object, expressed as a path from the root via intermediate fields to the value. Each segment in
   * this path consists of a field descriptor and an optional index (used for repeated fields, and set to zero for non-repeated fields).
   * 
   * Field value pointers are immutable, hashable, and define equals by value.
   */
  public static class FieldValuePointer {
    public final FieldDescriptor field;
    public final int index;
    public final FieldValuePointer parent;
    private final int hash;

    public FieldValuePointer(FieldValuePointer parent, FieldDescriptor field, int index) {
      this.parent = parent;
      this.field = field;
      this.index = index;
      this.hash = getHash();
    }

    private int getHash() {
      int hash = (parent == null) ? 13 : parent.getHash();
      hash ^= 0x9e3779b9 + (hash << 6) + (hash >> 2) + index;
      hash ^= 0x9e3779b9 + (hash << 6) + (hash >> 2) + field.getNumber();
      return hash;
    }

    // ======================================================================
    // Object methods
    @Override
    public String toString() {
      if (parent == null)
        return field.getName() + "[" + index + "]";
      else
        return parent.toString() + "." + field.getName() + "[" + index + "]";
    }

    @Override
    public int hashCode() {
      return hash;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null)
        return false;
      if (!(o instanceof FieldValuePointer))
        return false;
      FieldValuePointer other = (FieldValuePointer) o;
      return ((hash == other.hash) && (field == other.field) && (index == other.index) && (((parent == null) && (other.parent == null)) || ((parent != null) && parent
          .equals(other.parent))));
    }
  }
}