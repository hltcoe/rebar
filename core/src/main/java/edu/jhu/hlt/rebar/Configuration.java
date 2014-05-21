/**
 * Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */

/**
 * Class for reading and accessing configuration values in rebar.properties.
 */
package edu.jhu.hlt.rebar;

import java.io.File;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;

/**
 * Class for reading and accessing configuration values in rebar.properties.
 * 
 * Also configures the log4j logger based on properties defined in that file.
 * 
 */
public final class Configuration {
  
  public static final String envCfg = "REBAR_ENV";
  public static final String instanceCfg = "REBAR_INSTANCE";
  public static final String zookeeperCfg = "REBAR_ZOOKEEPERS";
  public static final String userCfg = "REBAR_USER";
  public static final String passwordCfg = "REBAR_PASSWORD";
  
  private static final String envString = System.getenv(envCfg);;
  private static final String instanceString = System.getenv(instanceCfg);
  private static final String zookeeperString = System.getenv(zookeeperCfg);
  private static final String userString = System.getenv(userCfg);
  private static final String passwordString = System.getenv(passwordCfg);
  
  public static boolean testingEnvSet() {
    return System.getenv("REBAR_ENV") == null;
  }
  
  public static boolean useAccumuloMock() {
    return envString != null && envString.equals("testing");
  }
  
  private static final String nonNullOrRTE(String prop, String val) {
    if (val == null)
      throw new RuntimeException("Property " + prop + " was not set.");
    return val;
  }

  public static String getAccumuloInstanceName() {
    return nonNullOrRTE(instanceCfg, instanceString);
  }

  public static String getZookeeperServer() {
    return nonNullOrRTE(zookeeperCfg, zookeeperString);
  }

  public static String getAccumuloUser() {
    return nonNullOrRTE(userCfg, userString);
  }

  public static byte[] getAccumuloPassword() {
    return nonNullOrRTE(passwordCfg, passwordString).getBytes();
  }

  public static boolean getTwitterTokenizerRw() {
    //return Boolean.parseBoolean(props.getProperty("TwitterTokenizer.rw"));
    return false;
  }

  public static Authorizations getAuths() {
    return Constants.NO_AUTHS;
  }
  
  public static PasswordToken getPasswordToken() {
    return new PasswordToken(getAccumuloPassword());
  }
  
  public static MiniAccumuloConfig getMiniConfig(File dir) throws RebarException {
    return new MiniAccumuloConfig(dir, "password");
  }
}
