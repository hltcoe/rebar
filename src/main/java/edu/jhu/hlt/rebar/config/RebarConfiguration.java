/**
 * Copyright 2012-2013 Johns Hopkins University HLTCOE. All rights reserved.
 * This software is released under the 2-clause BSD license.
 * See LICENSE in the project root directory.
 */

/**
 * Class for reading and accessing configuration values in rebar.properties.
 */
package edu.jhu.hlt.rebar.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * Class for reading and accessing configuration values in rebar.properties.
 * 
 * Also configures the log4j logger based on properties defined in that file.
 * 
 */
public final class RebarConfiguration {
    private static final Logger LOGGER = Logger.getLogger(RebarConfiguration.class);

    private static final Properties props;

    static {
        props = new Properties();
        InputStream stream = null;
        try {
            stream = RebarConfiguration.class.getClassLoader().getResourceAsStream("rebar.properties");
            if (stream == null)
                throw new RuntimeException("Problem finding rebar.properties on the classpath.");
            props.load(stream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load properties file rebar.properties!", e);
        } finally {
            try {
                stream.close();
            } catch (IOException ioe) {
                LOGGER.trace(ioe, ioe);
            }
        }
        // Configure the logger.
        PropertyConfigurator.configure(props);
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
}
