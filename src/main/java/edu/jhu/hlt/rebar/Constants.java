/**
 * 
 */
package edu.jhu.hlt.rebar;

/**
 * @author max
 *
 */
public class Constants {

  
  
  public static final String DOCUMENT_TABLE_NAME = "documents";
  public static final String DOCUMENT_COLF = "raw_doc";
  public static final String DOCUMENT_ANNOTATION_COLF = "annotations";
  
  public static final String AVAILABLE_CORPUS_TABLE_NAME = "available_corpora";
  public static final String CORPUS_PREFIX = "corpus_";
  
  public static final String STAGES_TABLE_NAME = "available_stages";
  public static final String STAGES_PREFIX = "stage_";
  public static final String STAGES_OBJ_COLF = "stage_obj";

  public static final String STAGES_DOCS_COLF = "documents";
  public static final String STAGES_DOCS_ANNOTATED_IDS_COLF = "annotated_ids";
  
  // Redis
  public static final String INGESTED_IDS_REDIS_KEY = "ingested-ids";

  /**
   * 
   */
  private Constants() {

  }
  
    
}
