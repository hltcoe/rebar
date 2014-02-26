package edu.jhu.hlt.rebar.util;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.jhu.hlt.rebar.util.FileUtil;

public class FileUtilTest {

  private Path path;
  private String testDir = "target/file_utils_test/";

  @Before
  public void setUp() throws Exception {
    path = Paths.get(this.testDir);
  }

  @After
  public void tearDown() throws Exception {
    Files.delete(Paths.get(this.testDir));
  }

  @Test
  public void testDeleteFolderAndSubfolders() throws IOException {
    Path bigPath = path.resolve("foo").resolve("qux").resolve("baz");
    Path otherPath = path.resolve("big").resolve("path");

    Files.createDirectories(bigPath);
    Files.createDirectories(otherPath);

    FileUtil.deleteFolderAndSubfolders(this.path.resolve("foo"));
    assertFalse(Files.exists(this.path.resolve("foo")));
    FileUtil.deleteFolderAndSubfolders(this.path.resolve("big"));
    assertFalse(Files.exists(this.path.resolve("big")));
  }

}
