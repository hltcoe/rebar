/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;

import edu.jhu.hlt.concrete.Annotator;
import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.LangId;
import edu.jhu.hlt.concrete.LanguagePrediction;
import edu.jhu.hlt.concrete.RebarThriftException;
import edu.jhu.hlt.concrete.Stage;
import edu.jhu.hlt.concrete.StageType;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.IllegalAnnotationException;
import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 * 
 */
public class RebarAnnotator extends AbstractAccumuloClient implements AutoCloseable, Annotator.Iface {

  private final RebarStageHandler ash;
  private static final Map<String, Set<String>> stageNameToIngestedIdSetMap;
  static {
    stageNameToIngestedIdSetMap = new HashMap<>();
  }

  /**
   * @throws RebarException
   * 
   */
  public RebarAnnotator() throws RebarException {
    this(Constants.getConnector());
  }

  public RebarAnnotator(Connector conn) throws RebarException {
    super(conn);
    this.ash = new RebarStageHandler(this.conn);
    for (Stage s : this.ash.getStagesInternal()) {
      String name = s.name;
      Set<String> annotatedIds = this.ash.getAnnotatedDocumentIds(s);
      stageNameToIngestedIdSetMap.put(name, annotatedIds);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see edu.jhu.hlt.rebar.accumulo.AbstractAccumuloClient#flush()
   */
  @Override
  public void flush() throws RebarException {
    try {
      this.bw.flush();
    } catch (MutationsRejectedException e) {
      throw new RebarException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see java.lang.AutoCloseable#close()
   */
  @Override
  public void close() throws Exception {
    this.bw.close();
  }

  private Set<String> prepareAnnotation(Communication comm, Stage stage) throws RebarException, IllegalAnnotationException {
    // the stage may not exist - if not, silently create.
    if (!this.ash.stageExistsInternal(stage.name)) {
      this.ash.createStageInternal(stage);
    }

    Set<String> ingestedIds;
    // if this stage was created after this class was loaded in CL,
    // we will need to update our cache.
    if (!stageNameToIngestedIdSetMap.containsKey(stage.name)) {
      ingestedIds = this.ash.getAnnotatedDocumentIds(stage);
      stageNameToIngestedIdSetMap.put(stage.name, ingestedIds);
    } else {
      // given the stage and document, it might have been previously annotated. check to see.
      ingestedIds = stageNameToIngestedIdSetMap.get(stage.name);
      if (ingestedIds.contains(comm.id))
        throw new IllegalAnnotationException("Communication: " + comm.id + " has been annotated with stage: " + stage.name + " previously.");
    }

    return ingestedIds;
  }
  
  public void addAnnotation(Communication comm, Stage stage, TBase<?,?> annotation) throws RebarException, IllegalAnnotationException, TException, MutationsRejectedException {
    Set<String> ingestedIds = this.prepareAnnotation(comm, stage);
    byte[] lidBytes = this.serializer.serialize(annotation);
    final Mutation m = new Mutation(comm.id);
    m.put(Constants.DOCUMENT_ANNOTATION_COLF, stage.name, new Value(lidBytes));
    this.bw.addMutation(m);
    this.ash.addAnnotatedDocument(stage, comm);
    ingestedIds.add(comm.id);
  }

  @Override
  public void addLanguageId(Communication comm, Stage stage, LangId lid) throws TException {
    if (stage.type != StageType.LANG_ID)
      throw new TException("Stage " + stage.name + " is of type " + stage.type.toString() + "; it cannot support a language ID annotation.");
    try {
      this.addAnnotation(comm, stage, lid);
    } catch (MutationsRejectedException | RebarException | IllegalAnnotationException e) {
      throw new TException(e);
    }
  }
  
  @Override
  public void addLanguagePrediction(Communication comm, Stage stage, LanguagePrediction lp) throws RebarThriftException, TException {
    if (stage.type != StageType.LANG_PRED)
      throw new TException("Stage " + stage.name + " is of type " + stage.type.toString() + "; it cannot support a language prediction annotation.");
    try {
      this.addAnnotation(comm, stage, lp);
    } catch (MutationsRejectedException | RebarException | IllegalAnnotationException e) {
      throw new TException(e);
    }
  }
}
