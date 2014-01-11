/**
  *  Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.rebar.scarbre.accumulo

object `package` {
  type BatchWriterOpts = org.apache.accumulo.core.cli.BatchWriterOpts
  type BatchWriterConfig = org.apache.accumulo.core.client.BatchWriterConfig
  type Connector = org.apache.accumulo.core.client.Connector
  type Scanner = org.apache.accumulo.core.client.Scanner;
  type BatchScanner = org.apache.accumulo.core.client.BatchScanner;
  type BatchWriter = org.apache.accumulo.core.client.BatchWriter;
  type Authorizations = org.apache.accumulo.core.security.Authorizations

  type Mutation = org.apache.accumulo.core.data.Mutation
  type Entry[K, V] = java.util.Map.Entry[K, V]
  type Key = org.apache.accumulo.core.data.Key;
  type Value = org.apache.accumulo.core.data.Value
  type Range = org.apache.accumulo.core.data.Range
  type Text = org.apache.hadoop.io.Text;

  type MockInstance = org.apache.accumulo.core.client.mock.MockInstance
  type ZooKeeperInstance = org.apache.accumulo.core.client.ZooKeeperInstance
  type Instance = org.apache.accumulo.core.client.Instance

  type PasswordToken = org.apache.accumulo.core.client.security.tokens.PasswordToken

  implicit def connectorToPowerConnector(conn: Connector) = new PowerConnector(conn)
  implicit def scanToPS(scan: Scanner) = new PowerScanner(scan)
  implicit def sToATNS(orig:String) = new AccumuloTableNameString(orig)
  implicit def m2pm (m: Mutation) = new PowerMutation(m)

  implicit val iBWC : BatchWriterConfig = AccumuloClient DefaultBWConfig
  implicit val iAuths : Authorizations = AccumuloClient DefaultAuths
}
