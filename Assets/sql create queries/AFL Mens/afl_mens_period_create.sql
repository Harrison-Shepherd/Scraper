CREATE TABLE afl_mens_period (
  -- Match Statistics
  marksInside50 INT DEFAULT NULL,
  handballs INT DEFAULT NULL,
  clangers INT DEFAULT NULL,
  hitoutsToAdvantage INT DEFAULT NULL,
  penalty50sAgainst INT DEFAULT NULL,
  disposals INT DEFAULT NULL,
  goalAssists INT DEFAULT NULL,
  kickEfficiency INT DEFAULT NULL,
  kicksEffective INT DEFAULT NULL,
  marksUncontested INT DEFAULT NULL,
  tackles INT DEFAULT NULL,
  freesFor INT DEFAULT NULL,
  behinds INT DEFAULT NULL,
  playerId VARCHAR(50) NOT NULL,
  goals INT DEFAULT NULL,
  inside50s INT DEFAULT NULL,
  disposalEfficiency INT DEFAULT NULL,
  period INT DEFAULT NULL,
  blocks INT DEFAULT NULL,
  squadId VARCHAR(50) NOT NULL,
  marks INT DEFAULT NULL,
  hitouts INT DEFAULT NULL,
  kicks INT DEFAULT NULL,
  marksContested INT DEFAULT NULL,
  possessionsContested INT DEFAULT NULL,
  freesAgainst INT DEFAULT NULL,
  clearances INT DEFAULT NULL,
  kicksIneffective INT DEFAULT NULL,
  possessionsUncontested INT DEFAULT NULL,

  -- Match Information
  matchId VARCHAR(50) NOT NULL,
  periodId VARCHAR(45) NOT NULL,  -- Precomputed from matchId and period

  -- Precomputed Columns
  uniqueMatchId VARCHAR(255) NOT NULL,
  uniquePlayerId VARCHAR(255) NOT NULL,
  uniquePeriodId VARCHAR(255) NOT NULL,

  -- Primary Key
  PRIMARY KEY (uniquePeriodId),

  -- Foreign Keys
  FOREIGN KEY (uniqueMatchId) REFERENCES afl_mens_match(uniqueMatchId),
  FOREIGN KEY (uniquePlayerId) REFERENCES player_info(uniquePlayerId)
);
