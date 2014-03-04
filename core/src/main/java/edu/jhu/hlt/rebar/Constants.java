/**
 * 
 */
package edu.jhu.hlt.rebar;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Value;

/**
 * @author max
 *
 */
public class Constants {

  public static final String DOCUMENT_TABLE_NAME = "documents";
  public static final String DOCUMENT_COLF = "raw_doc";
  public static final String DOCUMENT_ANNOTATION_COLF = "annotations";
  public static final String DOCUMENT_IDX_TABLE = "doc_idx";
  
  public static final String AVAILABLE_CORPUS_TABLE_NAME = "available_corpora";
  public static final String CORPUS_PREFIX = "corpus_";
  
  public static final String STAGES_TABLE_NAME = "available_stages";
  public static final String STAGES_PREFIX = "stage_";
  public static final String STAGES_OBJ_COLF = "stage_obj";

  public static final String STAGES_DOCS_COLF = "documents";
  public static final String STAGES_DOCS_ANNOTATED_IDS_COLF = "annotated_ids";
  
  // Redis
  public static final String INGESTED_IDS_REDIS_KEY = "ingested-ids";

  public static final Value EMPTY_VALUE = new Value(new byte[0]);
  
  /**
   * 
   */
  private Constants() {

  }
  
  public static Connector getConnector() throws RebarException {
    try {
      if (Configuration.useAccumuloMock()) {
        MockInstance inst = new MockInstance();
        return inst.getConnector("max", new PasswordToken(""));
      } else {
        Instance zki = new ZooKeeperInstance(Configuration.getAccumuloInstanceName(), Configuration.getZookeeperServer());
        return zki.getConnector(Configuration.getAccumuloUser(), Configuration.getPasswordToken());
      }
    } catch (AccumuloException | AccumuloSecurityException e) {
      throw new RebarException(e); 
    }
  }
}
