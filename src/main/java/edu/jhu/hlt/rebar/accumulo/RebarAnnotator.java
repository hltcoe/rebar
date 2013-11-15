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

import com.maxjthomas.dumpster.Annotator;
import com.maxjthomas.dumpster.Document;
import com.maxjthomas.dumpster.LangId;
import com.maxjthomas.dumpster.LanguagePrediction;
import com.maxjthomas.dumpster.RebarThriftException;
import com.maxjthomas.dumpster.Stage;

import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.IllegalAnnotationException;
import edu.jhu.hlt.rebar.RebarException;

/**
 * @author max
 * 
 */
public class RebarAnnotator extends AbstractAccumuloClient implements AutoCloseable, Annotator.Iface {

  private final AccumuloStageHandler ash;
  private static final Map<String, Set<String>> stageNameToIngestedIdSetMap;
  static {
    stageNameToIngestedIdSetMap = new HashMap<>();
  }

  /**
   * @throws RebarException
   * 
   */
  public RebarAnnotator() throws RebarException {
    this(AbstractAccumuloClient.getConnector());
  }

  public RebarAnnotator(Connector conn) throws RebarException {
    super(conn);
    this.ash = new AccumuloStageHandler(this.conn);
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

  private Set<String> prepareAnnotation(Document doc, Stage stage) throws RebarException, IllegalAnnotationException {
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
      if (ingestedIds.contains(doc.id))
        throw new IllegalAnnotationException("Document: " + doc.id + " has been annotated with stage: " + stage.name + " previously.");
    }

    return ingestedIds;
  }
  
  public void addAnnotation(Document document, Stage stage, TBase<?,?> annotation) throws RebarException, IllegalAnnotationException, TException, MutationsRejectedException {
    Set<String> ingestedIds = this.prepareAnnotation(document, stage);
    byte[] lidBytes = this.serializer.serialize(annotation);
    final Mutation m = new Mutation(document.id);
    m.put(Constants.DOCUMENT_ANNOTATION_COLF, stage.name, new Value(lidBytes));
    this.bw.addMutation(m);
    this.ash.addAnnotatedDocument(stage, document);
    ingestedIds.add(document.id);
  }

  @Override
  public void addLanguageId(Document document, Stage stage, LangId lid) throws TException {
    try {
      this.addAnnotation(document, stage, lid);
    } catch (MutationsRejectedException | RebarException | IllegalAnnotationException e) {
      throw new TException(e);
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see com.maxjthomas.dumpster.Annotator.Iface#addLanguagePrediction(com.maxjthomas.dumpster.Document, com.maxjthomas.dumpster.Stage,
   * com.maxjthomas.dumpster.LanguagePrediction)
   */
  @Override
  public void addLanguagePrediction(Document document, Stage stage, LanguagePrediction lp) throws RebarThriftException, TException {
    try {
      this.addAnnotation(document, stage, lp);
    } catch (MutationsRejectedException | RebarException | IllegalAnnotationException e) {
      throw new TException(e);
    }

  }
}
