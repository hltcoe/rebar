/**
 * 
 */
package edu.jhu.hlt.rebar.stage;

import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Value;
import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.TFieldIdEnum;

import edu.jhu.hlt.concrete.Communication;
import edu.jhu.hlt.concrete.validation.AbstractAnnotation;
import edu.jhu.hlt.rebar.AnnotationException;
import edu.jhu.hlt.rebar.Constants;
import edu.jhu.hlt.rebar.RebarException;
import edu.jhu.hlt.rebar.Util;
import edu.jhu.hlt.rebar.accumulo.AbstractCommunicationWriter;
import edu.jhu.hlt.rebar.accumulo.CommunicationReader;

/**
 * @author max
 * 
 */
public abstract class AbstractStageWriter<T extends TBase<T, ? extends TFieldIdEnum>> extends AbstractCommunicationWriter {

  protected final Stage stage;
  protected final CommunicationReader reader;

  /**
   * @throws RebarException
   * 
   */
  public AbstractStageWriter(Stage stage) throws RebarException {
    this(Constants.getConnector(), stage);
  }

  /**
   * @throws RebarException
   * 
   */
  public AbstractStageWriter(Connector conn, Stage stage) throws RebarException {
    super(conn);
    this.stage = stage;
    this.reader = new CommunicationReader(conn);
  }

  protected final void annotate(AbstractAnnotation<T> annotation, String docId) throws AnnotationException, RebarException {
    try {
      Communication c = this.reader.get(docId);
      if (annotation.validate(c)) {
        Mutation m = new Mutation(c.uuid);
        m.put("annotations", this.stage.getName(), new Value(this.serializer.serialize(annotation.getAnnotation())));

        this.idxBw.addMutation(Util.generateEmptyValueMutation("stage:"+this.stage.getName(), c.uuid, ""));
        this.bw.addMutation(m);
      } else {
        throw new AnnotationException("Your " + annotation.getClass().getName() + " object was invalid.");
      }
    } catch (TException | MutationsRejectedException e) {
      throw new RebarException(e);
    }
  }

  public abstract void annotate(T annotation, String docId) throws RebarException, AnnotationException;
  
//  public abstract Iterator<Communication> getDocuments() throws RebarException;
  
//  protected abstract T getViaColumnFamily(Map<Key, Value> decodedRow) throws TException, RebarException;
  
//  private Communication mergeSentenceSegmentationCollection(Communication c, Value v) throws IllegalAnnotationException, TException {
//    SentenceSegmentationCollection ssc = new SentenceSegmentationCollection();
//    this.deserializer.deserialize(ssc, v.get());
//    return mergeSentenceSegmentationCollection(c, ssc);
//  }
//
//  /**
//   * 
//   * @param c
//   * @param ssc
//   * @return a {@link Communication} object with {@link SentenceSegmentation}s merged in.
//   * @throws IllegalAnnotationException
//   */
//  private static Communication mergeSentenceSegmentationCollection(Communication c, SentenceSegmentationCollection ssc) throws IllegalAnnotationException {
//    Communication newC = new Communication(c);
//    List<SentenceSegmentation> ssList = ssc.getSentSegList();
//    Map<String, SentenceSegmentation> sectionIdToSentSegMap = new HashMap<>();
//    for (SentenceSegmentation ss : ssList)
//      sectionIdToSentSegMap.put(ss.getSectionId(), ss);
//
//    List<Section> sectList = newC.getSectionSegmentation().getSectionList();
//
//    // if section count != # of section segmentations, raise an exception - badly annotated data.
//    if (sectList.size() != ssList.size()) {
//      throw new IllegalAnnotationException("There must be an equal number of sections (" + sectList.size() + ") and sentence segmentations (" + ssList.size()
//          + ").");
//    }
//
//    for (Section s : sectList) {
//      String sId = s.getUuid();
//      if (sectionIdToSentSegMap.containsKey(sId)) {
//        s.setSentenceSegmentation(sectionIdToSentSegMap.get(sId));
//        sectionIdToSentSegMap.remove(sId);
//      } else
//        throw new IllegalAnnotationException("Section with ID: " + sId + " did not have a corresponding SectionSegmentation.");
//    }
//
//    if (!sectionIdToSentSegMap.isEmpty())
//      throw new IllegalAnnotationException("There were section segmentations that did not map to actual sections in the communications.");
//
//    return newC;
//  }
//
//  private Communication mergeTokenizationCollection(Communication c, Value v) throws IllegalAnnotationException, TException {
//    TokenizationCollection tc = new TokenizationCollection();
//    this.deserializer.deserialize(tc, v.get());
//    return mergeTokenizationCollection(c, tc);
//  }
//
//  private static Communication mergeTokenizationCollection(Communication c, TokenizationCollection tc) throws IllegalAnnotationException {
//    Communication newC = new Communication(c);
//
//    // create a map of Sentence ID --> Tokenization
//    Map<UUID, Tokenization> sentIdToTokenizationMap = new HashMap<>();
//    for (Tokenization t : tc.getTokenizationList())
//      sentIdToTokenizationMap.put(t.getSentenceId(), t);
//
//    // get list of sentences; iterate
//    List<Sentence> sentList = new ArrayList<>();
//    for (Section s : c.getSectionSegmentation().getSectionList()) {
//      List<Sentence> secSentList = s.getSentenceSegmentation().getSentenceList();
//      sentList.addAll(secSentList);
//    }
//
//    if (sentList.size() != sentIdToTokenizationMap.size())
//      throw new AnnotationException("There must be an equal number of sentences (" + sentList.size() + ") and tokenizations ("
//          + sentIdToTokenizationMap.size() + ").");
//
//    for (Sentence s : sentList) {
//      UUID sId = s.getUuid();
//      if (sentIdToTokenizationMap.containsKey(sId)) {
//        s.setTokenization(sentIdToTokenizationMap.get(sId));
//        sentIdToTokenizationMap.remove(sId);
//      } else {
//        throw new AnnotationException("A sentence did not have a tokenization [ID: " + sId.toString() + "].");
//      }
//    }
//
//    if (!sentIdToTokenizationMap.isEmpty())
//      throw new AnnotationException("There were tokenizations that did not map to sentences.");
//
//    return newC;
//  }
//
//  private Set<Communication> constructCommunicationSet(Stage s, Set<String> docIds) throws RebarException, TException, IOException, IllegalAnnotationException {
//    Set<Communication> docSet = new HashSet<>();
//
//    // we need to get a list of the dependency names so that if we see those stages,
//    // we can add them to the object.
//    Set<String> namesToGet = new HashSet<String>(s.dependencies);
//
//    // however, we also want to add the current stage name to get its annotations as well.
//    namesToGet.add(s.name);
//
//    // check our cache to make sure we have all of these stages - if not, update.
////    namesToGet.removeAll(stageNameToTypeMap.entrySet());
////    if (namesToGet.size() > 0)
////      this.updateCache();
//
//    BatchScanner bsc = this.createScanner(s, docIds);
//    Iterator<Entry<Key, Value>> iter = bsc.iterator();
//    while (iter.hasNext()) {
//      Entry<Key, Value> e = iter.next();
//      Map<Key, Value> rows = WholeRowIterator.decodeRow(e.getKey(), e.getValue());
//      Communication root = this.getRoot(rows);
//      EnumMap<StageType, Value> pendingMerges = new EnumMap<>(StageType.class);
//
//      boolean hasSectionSegmentations = false;
//      boolean hasSentenceSegmentations = false;
//      boolean hasTokenizations = false;
//      for (Entry<Key, Value> r : rows.entrySet()) {
//        Key k = r.getKey();
//        Value v = r.getValue();
//        String colQ = k.getColumnQualifier().toString();
//        StageType t = stageNameToTypeMap.get(colQ);
//        switch (t) {
//        case LANG_ID:
//          LanguageIdentification lid = new LanguageIdentification();
//          this.deserializer.deserialize(lid, v.get());
//          root.setLid(lid);
//          break;
//        case SECTION:
//          SectionSegmentation ss = new SectionSegmentation();
//          this.deserializer.deserialize(ss, v.get());
//          root.setSectionSegmentation(ss);
//          hasSectionSegmentations = true;
//          break;
//        case SENTENCE:
//          if (!hasSectionSegmentations) {
//            // if we haven't merged in the section segmentations, we'll need to resolve after iteration.
//            pendingMerges.put(t, v);
//          } else {
//            root = this.mergeSentenceSegmentationCollection(root, v);
//            hasSentenceSegmentations = true;
//          }
//          break;
//        case TOKENIZATION:
//          if (!hasSectionSegmentations || !hasSentenceSegmentations) {
//            pendingMerges.put(t, v);
//          } else {
//            root = this.mergeTokenizationCollection(root, v);
//            hasTokenizations = true;
//          }
//
//          break;
//        default:
//          throw new IllegalArgumentException("Case: " + s.type.toString() + " not handled yet.");
//        }
//      }
//
//      // deal with remaining stages that were out of order.
//      if (!pendingMerges.isEmpty()) {
//        if (pendingMerges.containsKey(StageType.SENTENCE)) {
//          root = this.mergeSentenceSegmentationCollection(root, pendingMerges.get(StageType.SENTENCE));
//          pendingMerges.remove(StageType.SENTENCE);
//        }
//
//        if (pendingMerges.containsKey(StageType.TOKENIZATION)) {
//          root = this.mergeTokenizationCollection(root, pendingMerges.get(StageType.TOKENIZATION));
//          pendingMerges.remove(StageType.TOKENIZATION);
//        }
//      }
//
//      docSet.add(root);
//    }
//
//    return docSet;
//  }
//
//  private Communication getRoot(Map<Key, Value> decodedRow) throws TException {
//    Communication d = new Communication();
//    Iterator<Entry<Key, Value>> iter = decodedRow.entrySet().iterator();
//    while (iter.hasNext()) {
//      Entry<Key, Value> entry = iter.next();
//      if (entry.getKey().compareColumnFamily(new Text(Constants.DOCUMENT_COLF)) == 0) {
//        this.deserializer.deserialize(d, entry.getValue().get());
//        iter.remove();
//      }
//    }
//
//    return d;
//  }
}
