/**
  *  Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
  *  This software is released under the 2-clause BSD license.
  *  See LICENSE in the project root directory.
  */
package edu.jhu.hlt.rebar
package accumulo

import org.apache.accumulo.core.client.admin.TableOperations

/**
  * Thin wrapper around `TableOperations` for easier use.
  */
object TableOps {
  import scala.util.{Try, Success, Failure}

  private val tableOps = AccumuloClient.DefaultConnector.tableOperations

  /**
    * Create an accumulo table.
    *
    * @param tableName The name of the table to create.
    * @return A `Try` with any exceptions.
    */
  def create(tableName: String)(implicit conn: Connector) : Try[Unit] =
    Try(conn.tableOperations.create(tableName))

  /**
    * Delete an accumulo table.
    *
    * @param tableName The name of the table to create.
    * @return A `Try` with any exceptions.
    */
  def delete(tableName: String)(implicit conn: Connector) : Try[Unit] =
    Try(conn.tableOperations.delete(tableName))

  /**
    * Check to see if an accumulo table exists.
    *
    * @param tableName The name of the table to create.
    * @return A `Try` with the enclosed `Boolean`.
    */
  def exists(tableName: String)(implicit conn: Connector) : Try[Boolean] =
    Try(conn.tableOperations.exists(tableName))


  def checkExistsAndCreate(tableName: String)(implicit conn: Connector) : Try[Unit] = {
    if (!exists(tableName).getOrElse(false))
      create(tableName)
    else
      Success()
  }
}
