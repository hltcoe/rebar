/*
 * 
 */
package edu.jhu.hlt.rebar.stage;

import java.util.HashSet;
import java.util.Set;

import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.InvalidStageNameException;

/**
 * @author max
 *
 */
public class Stage {

  private String name;
  private String description;
  private long createTime;
  private Set<String> dependencies;
  private StageType stageType;

  /**
   * @throws InvalidStageNameException 
   * 
   */
  public Stage(String name, String description, StageType stageType, Set<String> dependencies) throws InvalidStageNameException {
    this(name, description, System.currentTimeMillis(), stageType, dependencies);
  }
  
  public Stage(String name, String description, long createTime, StageType stageType, Set<String> dependencies) throws InvalidStageNameException {
    if (!Stage.isValidStageName(name))
      throw new InvalidStageNameException(name);
    
    this.name = name;
    this.description = description;
    this.stageType = stageType;
    this.dependencies = new HashSet<>(dependencies);
    this.createTime = createTime;
  }
  
  public Stage() { };

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the description
   */
  public String getDescription() {
    return description;
  }

  /**
   * @return the createTime
   */
  public long getCreateTime() {
    return createTime;
  }

  /**
   * @return the dependencies
   */
  public Set<String> getDependencies() {
    return dependencies;
  }

  /**
   * @return the stageType
   */
  public StageType getStageType() {
    return stageType;
  }

  public static boolean isValidStageName(String stageName) {
    return stageName.startsWith(Constants.STAGES_PREFIX);
  }

  /**
   * @param name the name to set
   * @throws InvalidStageNameException 
   */
  public void setName(String name) throws InvalidStageNameException {
    if (!isValidStageName(name))
      throw new InvalidStageNameException(name);
    this.name = name;
  }

  /**
   * @param description the description to set
   */
  public void setDescription(String description) {
    this.description = description;
  }

  /**
   * @param createTime the createTime to set
   */
  public void setCreateTime(long createTime) {
    this.createTime = createTime;
  }

  /**
   * @param dependencies the dependencies to set
   */
  public void setDependencies(Set<String> dependencies) {
    this.dependencies = dependencies;
  }

  /**
   * @param stageType the stageType to set
   */
  public void setStageType(StageType stageType) {
    this.stageType = stageType;
  }
}
