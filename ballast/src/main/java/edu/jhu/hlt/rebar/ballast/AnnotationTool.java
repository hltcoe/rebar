/*
 * Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */
package edu.jhu.hlt.rebar.ballast;

import org.apache.thrift.TBase;
import org.apache.thrift.TFieldIdEnum;

import edu.jhu.hlt.concrete.Communication;

/**
 * @author max
 *
 */
public interface AnnotationTool<T extends TBase<T, ? extends TFieldIdEnum>> {
  public T annotate(Communication c) throws AnnotationException;
}
