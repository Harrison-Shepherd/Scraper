CREATE TABLE fast5_mens_match (
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
  goalMisses INT DEFAULT NULL,
  blocked INT DEFAULT NULL,
  deflectionWithGain INT DEFAULT NULL,
  goalAssists INT DEFAULT NULL,
  tossUpWin INT DEFAULT NULL,
  centrePassReceives INT DEFAULT NULL,
  obstructionPenalties INT DEFAULT NULL,
  feeds INT DEFAULT NULL,

  -- Player Information
  playerId VARCHAR(50) NOT NULL,
  goals INT DEFAULT NULL,
  offsides INT DEFAULT NULL,
  secondPhaseReceive INT DEFAULT NULL,
  badPasses INT DEFAULT NULL,
  breaks INT DEFAULT NULL,
  blocks INT DEFAULT NULL,
  badHands INT DEFAULT NULL,
  missedGoalTurnover INT DEFAULT NULL,
  squadId VARCHAR(50) NOT NULL,
  deflectionPossessionGain INT DEFAULT NULL,
  possessionChanges INT DEFAULT NULL,
  startingPositionCode VARCHAR(3) DEFAULT NULL,
  goalAttempts INT DEFAULT NULL,
  goalAttempts3 INT DEFAULT NULL,
  goalAttempts2 INT DEFAULT NULL,
  contactPenalties INT DEFAULT NULL,
  goalAttempts1 INT DEFAULT NULL,
  quartersPlayed INT DEFAULT NULL,
  minutesPlayed INT DEFAULT NULL,
  feedWithAttempt INT DEFAULT NULL,
  unforcedTurnovers INT DEFAULT NULL,
  pickups INT DEFAULT NULL,
  goals1 INT DEFAULT NULL,
  currentPositionCode VARCHAR(3) DEFAULT NULL,
  gainToGoalPerc INT DEFAULT NULL,
  intercepts INT DEFAULT NULL,
  goals3 INT DEFAULT NULL,
  goals2 INT DEFAULT NULL,
  fixtureYear VARCHAR(50) NOT NULL,

  -- Additional Information
  shortDisplayName VARCHAR(50) DEFAULT NULL,
  firstname VARCHAR(50) DEFAULT NULL,  
  surname VARCHAR(50) DEFAULT NULL,
  displayName VARCHAR(50) DEFAULT NULL,
  squadName VARCHAR(100) DEFAULT NULL,

  -- Match Information
  homeId VARCHAR(50) NOT NULL,
  awayId VARCHAR(50) NOT NULL,
  opponent VARCHAR(100) DEFAULT NULL,
  round INT DEFAULT NULL,
  fixtureId VARCHAR(50) NOT NULL,
  sportId VARCHAR(50) NOT NULL,
  matchId VARCHAR(50) NOT NULL,

  -- Precomputed Columns (calculated in the application layer)
  uniqueFixtureId VARCHAR(255) NOT NULL,
  uniquePlayerId VARCHAR(255) NOT NULL,
  uniqueSquadId VARCHAR(255) NOT NULL,
  uniqueSportId VARCHAR(255) NOT NULL,
  uniqueMatchId VARCHAR(255) NOT NULL,

  -- Primary Key
  PRIMARY KEY (uniqueMatchId),

  -- Foreign Keys
  FOREIGN KEY (uniqueFixtureId) REFERENCES fast5_mens_fixture(uniqueFixtureId),
  FOREIGN KEY (uniquePlayerId) REFERENCES player_info(uniquePlayerId),
  FOREIGN KEY (uniqueSquadId) REFERENCES squad_info(uniqueSquadId),
  FOREIGN KEY (uniqueSportId) REFERENCES sport_info(uniqueSportId)
);
