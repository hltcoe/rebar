/**
 * Copyright 2012-2014 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */

/**
 * Class for reading and accessing configuration values in rebar.properties.
 */
package edu.jhu.hlt.rebar;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.PropertyConfigurator;

/**
 * Class for reading and accessing configuration values in rebar.properties.
 * 
 * Also configures the log4j logger based on properties defined in that file.
 * 
 */
public final class Configuration {
  private static final Properties props = new Properties();
  
  static {
    if (System.getenv("REBAR_ENV").equals("testing"))
      props.setProperty("accumuloMock", "true");
    else {
      try (InputStream stream = Configuration.class.getClassLoader().getResourceAsStream("rebar.properties");) {
        if (stream == null)
          throw new RuntimeException("Problem finding rebar.properties on the classpath.");
        props.load(stream);
      } catch (IOException e) {
        throw new RuntimeException("Failed to load properties file rebar.properties!", e);
      }
      // Configure the logger.
      PropertyConfigurator.configure(props);
    }
  };

  public static boolean useAccumuloMock() {
    return Boolean.parseBoolean(props.getProperty("accumuloMock"));
  }

  public static String getAccumuloInstanceName() {
    return props.getProperty("accumuloInstanceName");
  }

  public static String getZookeeperServer() {
    return props.getProperty("zookeeperServer");
  }

  public static String getAccumuloUser() {
    return props.getProperty("accumuloUser");
  }

  public static byte[] getAccumuloPassword() {
    return props.getProperty("accumuloPassword").getBytes();
  }

  public static String getMySqlUsername() {
    return props.getProperty("mySqlUsername");
  }

  public static String getMySqlPassword() {
    return props.getProperty("mySqlPassword");
  }

  public static String getHdfsRoot() {
    return props.getProperty("hdfsRoot");
  }

  public static boolean getTwitterTokenizerRw() {
    return Boolean.parseBoolean(props.getProperty("TwitterTokenizer.rw"));
  }

  public static Path getFileCorpusDirectory() {
    return Paths.get(props.getProperty("fileCorpusDirectory"));
  }

  public static Path getTestFileCorpusDirectory() {
    return Paths.get("target/file-corpora-test");
  }
  
  public static Authorizations getAuths() {
    return Constants.NO_AUTHS;
  }
  
  public static PasswordToken getPasswordToken() {
    return new PasswordToken(getAccumuloPassword());
  }
}
