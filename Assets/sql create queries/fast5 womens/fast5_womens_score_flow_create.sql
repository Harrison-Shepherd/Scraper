CREATE TABLE fast5_womens_score_flow (
  -- Period and Timing Information
  period INT DEFAULT NULL,
  periodSeconds INT DEFAULT NULL,

  -- Scoring Information
  distanceCode INT DEFAULT NULL,
  scorepoints INT DEFAULT NULL,
  scoreName VARCHAR(45) DEFAULT NULL,

  -- Player and Position Information
  positionCode VARCHAR(45) DEFAULT NULL,
  squadId VARCHAR(50) NOT NULL,
  playerId VARCHAR(50) NOT NULL,

  -- Match Information
  matchId VARCHAR(50) NOT NULL,

  -- Unique Identifier
  scoreFlowId VARCHAR(45) NOT NULL,  -- Precomputed from matchId and index (or other unique logic)

  -- Precomputed Columns (calculated in the application layer)
  uniquePlayerId VARCHAR(255) NOT NULL,
  uniqueMatchId VARCHAR(255) NOT NULL,

  -- Primary Key
  PRIMARY KEY (scoreFlowId),

  -- Foreign Key
  FOREIGN KEY (uniqueMatchId) REFERENCES fast5_womens_match(uniqueMatchId),

  -- Info Foreign Keys
  FOREIGN KEY (uniquePlayerId) REFERENCES player_info(uniquePlayerId)
);
