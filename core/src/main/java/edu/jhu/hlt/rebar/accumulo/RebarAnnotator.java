/**
 * 
 */
package edu.jhu.hlt.rebar.accumulo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.EntityMentionSet;
import edu.jhu.hlt.concrete.EntitySet;
import edu.jhu.hlt.concrete.LanguageIdentification;

import edu.jhu.hlt.rebar.RebarThriftException;
import edu.jhu.hlt.concrete.Section;
import edu.jhu.hlt.concrete.SectionSegmentation;
import edu.jhu.hlt.concrete.SentenceSegmentation;
import edu.jhu.hlt.concrete.SentenceSegmentationCollection;
import edu.jhu.hlt.rebar.Stage;
import edu.jhu.hlt.rebar.StageType;
import edu.jhu.hlt.concrete.TokenizationCollection;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.IllegalAnnotationException;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.util.BooleanToStringTuple;

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

  private RebarThriftException generateAnnotationException(StageType type, Exception ex) {
    return new RebarThriftException("There was an error adding an annotation of stage: " + type.toString() + ": " + ex.getMessage());
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

  public void addAnnotation(Communication comm, Stage stage, TBase<?, ?> annotation) throws RebarException, IllegalAnnotationException, TException,
      MutationsRejectedException {
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
      throw new RebarThriftException("Stage " + stage.name + " is of type " + stage.type.toString() + "; it cannot support a language ID annotation.");
    try {
      this.addAnnotation(comm, stage, lid);
    } catch (MutationsRejectedException | RebarException | IllegalAnnotationException e) {
      throw new RebarThriftException(e.getMessage());
    }
  }

  @Override
  public void addLanguagePrediction(Communication comm, Stage stage, LanguagePrediction lp) throws RebarThriftException, TException {
    if (stage.type != StageType.LANG_PRED)
      throw new RebarThriftException("Stage " + stage.name + " is of type " + stage.type.toString() + "; it cannot support a language prediction annotation.");
    try {
      this.addAnnotation(comm, stage, lp);
    } catch (MutationsRejectedException | RebarException | IllegalAnnotationException e) {
      throw new RebarThriftException(e.getMessage());
    }
  }

  /*
   * (non-Javadoc)
   * 
   * @see edu.jhu.hlt.concrete.Annotator.Iface#addSectionSegmentation(edu.jhu.hlt.concrete.Communication, edu.jhu.hlt.concrete.Stage,
   * edu.jhu.hlt.concrete.SectionSegmentation)
   */
  @Override
  public void addSectionSegmentation(Communication comm, Stage stage, SectionSegmentation sectionSegmentation) throws RebarThriftException, TException {
    if (!isValidSectionSegmentation(sectionSegmentation))
      throw new RebarThriftException("This is not a valid SectionSegmentation object; it has no sections.");

    if (stage.type != StageType.SECTION)
      throw new RebarThriftException("The type of this stage must be Section, not: " + stage.type.toString());

    try {
      this.addAnnotation(comm, stage, sectionSegmentation);
    } catch (MutationsRejectedException | RebarException | IllegalAnnotationException e) {
      throw new RebarThriftException("There was an error during ingest: " + e.getMessage());
    }
  }

  public static boolean isValidSectionSegmentation(SectionSegmentation ss) {
    List<Section> secList = ss.getSectionList();
    if (secList == null || secList.size() == 0)
      return false;
    return true;
  }

  public static BooleanToStringTuple isValidSentenceSegmentationCollection(Communication c, SentenceSegmentationCollection ssc) {
    List<SentenceSegmentation> ssList = ssc.getSentSegList();
    Map<String, SentenceSegmentation> sectionIdToSentSegMap = new HashMap<>();
    for (SentenceSegmentation ss : ssList)
      sectionIdToSentSegMap.put(ss.getSectionId().getId(), ss);

    List<Section> sectList = c.getSectionSegmentation().getSectionList();

    // if section count != # of section segmentations, it's not valid.
    int sectListSize = sectList.size();
    int sentSegListSize = ssList.size();
    if (sectList.size() != ssList.size()) {
      return new BooleanToStringTuple(false, "The number of sections [" + sectListSize + "] did not equal the number of sentence segmentations ["
          + sentSegListSize + "].");
    }

    // attempt to match each section segmentation with a section.
    // if we have extras on either side, return false - not valid; means there are unmapped sections.
    for (Section s : sectList) {
      String sId = s.getUuid().getId();
      if (sectionIdToSentSegMap.containsKey(sId)) {
        sectionIdToSentSegMap.remove(sId);
      } else
        return new BooleanToStringTuple(false, "A section ID [" + sId + "] did not map to the list of sentence segmentations.");
    }

    // if we have anything left, there are unmapped section segmentations - not valid.
    if (!sectionIdToSentSegMap.isEmpty()) {
      StringBuilder sb = new StringBuilder();
      for (String idStr : sectionIdToSentSegMap.keySet()) {
        sb.append(" ");
        sb.append(idStr);
      }

      return new BooleanToStringTuple(false, "One or more sentence segmentation IDs did not map to the list of sections. They are: [" + sb.toString() + " ]");
    }

    return new BooleanToStringTuple(true, "OK");
  }

  @Override
  public void addSentenceSegmentations(Communication comm, Stage stage, SentenceSegmentationCollection sentenceSegmentationList) throws RebarThriftException,
      TException {
    if (stage.type != StageType.SENTENCE)
      throw new RebarThriftException("The type of this stage must be Sentence, not: " + stage.type.toString());

    BooleanToStringTuple t = isValidSentenceSegmentationCollection(comm, sentenceSegmentationList);
    if (!t.getBoolean())
      throw new RebarThriftException("Your sentence segmentation had an issue: " + t.getMessage());

    try {
      this.addAnnotation(comm, stage, sentenceSegmentationList);
    } catch (MutationsRejectedException | RebarException | IllegalAnnotationException e) {
      throw generateAnnotationException(StageType.SENTENCE, e);
    }
  }

  @Override
  public void addTokenizations(Communication comm, Stage stage, TokenizationCollection tokenizations) throws RebarThriftException, TException {
    if (stage.type != StageType.TOKENIZATION)
      throw new RebarThriftException("The type of this stage must be Tokenization, not: " + stage.type.toString());

    try {
      this.addAnnotation(comm, stage, tokenizations);
    } catch (MutationsRejectedException | RebarException | IllegalAnnotationException e) {
      throw generateAnnotationException(StageType.TOKENIZATION, e);
    }
  }

  @Override
  public void addEntities(Communication comm, Stage stage, EntitySet es) throws RebarThriftException, TException {
    // TODO Auto-generated method stub
    if (stage.type != StageType.ENTITIES)
      throw new RebarThriftException("The type of this stage must be " + StageType.ENTITIES.toString() + ", not: " + stage.type.toString());

    try {
      this.addAnnotation(comm, stage, es);
    } catch (MutationsRejectedException | RebarException | IllegalAnnotationException e) {
      throw generateAnnotationException(StageType.ENTITIES, e);
    }
  }

  @Override
  public void addEntityMentions(Communication comm, Stage stage, EntityMentionSet ems) throws RebarThriftException, TException {
    // TODO Auto-generated method stub
    if (stage.type != StageType.ENTITY_MENTIONS)
      throw new RebarThriftException("The type of this stage must be " + StageType.ENTITY_MENTIONS.toString() + ", not: " + stage.type.toString());

    try {
      this.addAnnotation(comm, stage, ems);
    } catch (MutationsRejectedException | RebarException | IllegalAnnotationException e) {
      throw generateAnnotationException(StageType.ENTITY_MENTIONS, e);
    }
  }
}
