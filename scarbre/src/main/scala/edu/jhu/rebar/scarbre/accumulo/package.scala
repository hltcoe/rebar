/**
  *  Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.rebar.scarbre.accumulo

object `package` {
  type BatchWriterOpts = org.apache.accumulo.core.cli.BatchWriterOpts
  type Connector = org.apache.accumulo.core.client.Connector

  type Mutation = org.apache.accumulo.core.data.Mutation
  type Value = org.apache.accumulo.core.data.Value
}
