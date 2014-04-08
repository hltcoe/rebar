include "stage.thrift"
include "language.thrift"
include "structure.thrift"
include "entities.thrift"
include "situations.thrift"
include "ex.thrift"
include "email.thrift"
include "twitter.thrift"
include "audio.thrift"
include "communication.thrift"

namespace java edu.jhu.hlt.asphalt.services
namespace py asphalt.services
#@namespace scala edu.jhu.hlt.grommet.services

typedef communication.Communication Comm

service Ingester {
  void ingest(1: communication.Communication comm) throws (1: ex.AsphaltException ex)
}

/**
 * A service definition for manipulating stages.
 */
service StageHandler {
  bool stageExists(1: string stageName) throws (1: ex.AsphaltException exc)
  void createStage(1: stage.Stage stage) throws (1: ex.AsphaltException exc)
  list<stage.Stage> getStages() throws (1: ex.AsphaltException exc)
}

/**
 * Service for handling corpora.
 */
service CorpusHandler {
  void createCorpus(1: string corpusName, 2: list<communication.Communication> commList) throws (1: ex.AsphaltException ex)
  list<communication.Communication> getCorpusCommunicationSet(1: string corpusName) throws (1: ex.AsphaltException ex)
  list<string> listCorpora() throws (1: ex.AsphaltException ex)
  void deleteCorpus(1: string corpusName) throws (1: ex.AsphaltException ex)
  bool corpusExists(1: string corpusName) throws (1: ex.AsphaltException ex)
}

/**
 * Annotator service methods.
 */
service Annotator {
  structure.SentenceSegmentationCollection generateSentenceSegmentationCollection(1: Comm c) throws (1: ex.AsphaltException ex)
  structure.TokenizationCollection generateTokenizationCollection(1: Comm c) throws (1: ex.AsphaltException ex)
  entities.EntityMentionSet generateEntityMentionSet(1: Comm c) throws (1: ex.AsphaltException ex)
  entities.EntitySet generateEntitySet(1: Comm c) throws (1: ex.AsphaltException ex)
}