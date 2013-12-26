/**
  *  Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.rebar.scarbre
package accumulo

import org.apache.accumulo.core.client.admin.TableOperations

/**
  * Thin wrapper around `TableOperations` for easier use.
  * 
  * @constructor pass in a `Connector` object for Accumulo usage.
  */
class TableOps(conn: Connector) {
  val tableOps = conn.tableOperations

  def createTable(tableName: String) = {
    tableOps.create(tableName)
  }

  def deleteTable(tableName: String) = {
    tableOps.delete(tableName)
  }

  def tableExists(tableName: String) = {
    tableOps.exists(tableName)
  }
}
