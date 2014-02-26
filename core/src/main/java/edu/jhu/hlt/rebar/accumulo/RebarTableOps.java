/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */
package edu.jhu.hlt.rebar.accumulo;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.client.admin.TableOperations;

import edu.jhu.hlt.rebar.RebarException;

/**
 * Thin wrapper around {@link TableOperations} for easier use.
 * 
 * @author max
 */
public class RebarTableOps {

  private final TableOperations tableOps;

  public RebarTableOps(Connector conn) {
    this.tableOps = conn.tableOperations();
  }

  public void createTableIfNotExists(String tableName) throws RebarException {
    try {
      if (!this.tableOps.exists(tableName)) {
        this.tableOps.create(tableName);
      }
    } catch (AccumuloException e) {
      throw new RebarException(e);
    } catch (AccumuloSecurityException e) {
      throw new RebarException(e);
    } catch (TableExistsException e) {
      throw new RebarException(e);
    }
  }

  public void deleteTable(String tableName) throws RebarException {
    try {
      this.tableOps.delete(tableName);
    } catch (AccumuloException e) {
      throw new RebarException(e);
    } catch (AccumuloSecurityException e) {
      throw new RebarException(e);
    } catch (TableNotFoundException e) {
      throw new RebarException(e);
    }
  }

  public boolean tableExists(String tableName) {
    return this.tableOps.exists(tableName);
  }
}
