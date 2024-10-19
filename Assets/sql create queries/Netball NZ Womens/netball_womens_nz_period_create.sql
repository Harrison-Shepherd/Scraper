CREATE TABLE netball_womens_nz_period (
  -- Match Statistics
  rebounds INT DEFAULT NULL,
  turnoverHeld INT DEFAULT NULL,
  centrePassToGoalPerc INT DEFAULT NULL,
  penalties INT DEFAULT NULL,
  deflectionWithNoGain INT DEFAULT NULL,
  generalPlayTurnovers INT DEFAULT NULL,
  interceptPassThrown INT DEFAULT NULL,
  gain INT DEFAULT NULL,
  points INT DEFAULT NULL,
  netPoints INT DEFAULT NULL,  
  goalMisses INT DEFAULT NULL,
  blocked INT DEFAULT NULL,
  deflectionWithGain INT DEFAULT NULL,
  deflections INT DEFAULT NULL,  
  defensiveRebounds INT DEFAULT NULL,  
  offensiveRebounds INT DEFAULT NULL,  
  goalAssists INT DEFAULT NULL,
  tossUpWin INT DEFAULT NULL,
  centrePassReceives INT DEFAULT NULL,
  obstructionPenalties INT DEFAULT NULL,
  feeds INT DEFAULT NULL,
  passes INT DEFAULT NULL,  
  
  -- Player Information
  playerId VARCHAR(50) NOT NULL,
  squadId VARCHAR(50) NOT NULL,
  startingPositionCode VARCHAR(45) DEFAULT NULL,
  currentPositionCode VARCHAR(45) DEFAULT NULL,
  
  -- Performance Statistics
  goals INT DEFAULT NULL,
  offsides INT DEFAULT NULL,
  secondPhaseReceive INT DEFAULT NULL,
  badPasses INT DEFAULT NULL,
  period VARCHAR(45) NOT NULL,
  breaks INT DEFAULT NULL,
  blocks INT DEFAULT NULL,
  badHands INT DEFAULT NULL,
  missedGoalTurnover INT DEFAULT NULL,
  turnovers INT DEFAULT NULL,  
  possessionChanges INT DEFAULT NULL,
  goalAttempts INT DEFAULT NULL,
  contactPenalties INT DEFAULT NULL,
  quartersPlayed INT DEFAULT NULL,
  minutesPlayed INT DEFAULT NULL,
  feedWithAttempt INT DEFAULT NULL,
  unforcedTurnovers INT DEFAULT NULL,
  pickups INT DEFAULT NULL,
  gainToGoalPerc INT DEFAULT NULL,
  intercepts INT DEFAULT NULL,

  -- Match Information
  matchId VARCHAR(50) NOT NULL,  -- Reference to the match
  periodId VARCHAR(45) NOT NULL,  -- Precomputed in the application code

  -- Precomputed Columns (calculated in the application layer)
  uniqueMatchId VARCHAR(255) NOT NULL,
  uniquePlayerId VARCHAR(255) NOT NULL,
  uniquePeriodId VARCHAR(255) NOT NULL,

  -- Primary Key
  PRIMARY KEY (uniquePeriodId),

  -- Foreign Keys
  FOREIGN KEY (uniqueMatchId) REFERENCES netball_womens_nz_match(uniqueMatchId),  -- Foreign key linking to match table
  FOREIGN KEY (uniquePlayerId) REFERENCES player_info(uniquePlayerId)
);
