/**
 * 
 */
package edu.jhu.hlt.rebar.file;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;

import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Stage;

/**
 * @author max
 * 
 */
public class FileStage implements Stage {

  private final String name;
  private final String version;
  private final int stageId;
  // private final FileBackedCorpus owner;
  private final Path ownerPath;
  private final Set<Stage> dependencies;
  private final String description;
  private boolean isPublic;

  private final Path stagePath;

  /**
     * 
     */
  public FileStage(String name, String version, int stageId, Path ownerPath, Set<Stage> dependencies, String description, boolean isPublic)
      throws RebarException {
    this.name = name;
    this.version = version;
    this.stageId = stageId;
    this.ownerPath = ownerPath;
    this.dependencies = dependencies;
    this.description = description;
    this.isPublic = isPublic;

    this.stagePath = this.ownerPath.resolve("stages").resolve(this.name).resolve(this.version);
    try {
      Files.createDirectories(this.stagePath);
    } catch (IOException ioe) {
      throw new RebarException(ioe);
    }
  }

  /**
   * 
   * @return
   */
  public Path getPath() {
    return this.stagePath;
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.Comparable#compareTo(java.lang.Object)
   */
  @Override
  public int compareTo(Stage o) {
    return Integer.compare(this.stageId, o.getStageId());
  }

  /*
   * (non-Javadoc)
   * 
   * @see edu.jhu.rebar.Stage#getStageName()
   */
  @Override
  public String getStageName() {
    return this.name;
  }

  /*
   * (non-Javadoc)
   * 
   * @see edu.jhu.rebar.Stage#getStageVersion()
   */
  @Override
  public String getStageVersion() {
    return this.version;
  }

  /*
   * (non-Javadoc)
   * 
   * @see edu.jhu.rebar.Stage#getStageId()
   */
  @Override
  public int getStageId() {
    return this.stageId;
  }

  /*
   * (non-Javadoc)
   * 
   * @see edu.jhu.rebar.Stage#isPublic()
   */
  @Override
  public boolean isPublic() {
    return this.isPublic;
  }

  /*
   * (non-Javadoc)
   * 
   * @see edu.jhu.rebar.Stage#getDependencies()
   */
  @Override
  public Set<Stage> getDependencies() {
    return this.dependencies;
  }

  /*
   * (non-Javadoc)
   * 
   * @see edu.jhu.rebar.Stage#getDescription()
   */
  @Override
  public String getDescription() {
    return this.description;
  }

}
