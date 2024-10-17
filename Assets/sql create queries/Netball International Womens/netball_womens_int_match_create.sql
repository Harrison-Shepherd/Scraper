CREATE TABLE netball_womens_international_match (
  -- Match Statistics
  rebounds INT DEFAULT NULL,
  penalties INT DEFAULT NULL,
  gain INT DEFAULT NULL,
  possessions INT DEFAULT NULL,
  offensiveRebounds INT DEFAULT NULL,
  goalMisses INT DEFAULT NULL,
  blocked INT DEFAULT NULL,
  passes INT DEFAULT NULL,
  goalAssists INT DEFAULT NULL,
  disposals INT DEFAULT NULL,
  tossUpWin INT DEFAULT NULL,
  centrePassReceives INT DEFAULT NULL,
  obstructionPenalties INT DEFAULT NULL,
  feeds INT DEFAULT NULL,

  -- Player Information
  playerId VARCHAR(50) NOT NULL,
  squadId VARCHAR(50) NOT NULL,
  startingPositionCode VARCHAR(45) DEFAULT NULL,
  currentPositionCode VARCHAR(45) DEFAULT NULL,

  -- Performance Statistics
  goals INT DEFAULT NULL,
  offsides INT DEFAULT NULL,
  badPasses INT DEFAULT NULL,
  defensiveRebounds INT DEFAULT NULL,
  breaks INT DEFAULT NULL,
  blocks INT DEFAULT NULL,
  badHands INT DEFAULT NULL,
  missedGoalTurnover INT DEFAULT NULL,
  turnovers INT DEFAULT NULL,
  goalAttempts INT DEFAULT NULL,
  deflections INT DEFAULT NULL,
  contactPenalties INT DEFAULT NULL,
  quartersPlayed INT DEFAULT NULL,
  minutesPlayed INT DEFAULT NULL,
  pickups INT DEFAULT NULL,
  intercepts INT DEFAULT NULL,

  goal1 INT DEFAULT NULL,  
  goal2 INT DEFAULT NULL,  
  attempt1 INT DEFAULT NULL,  
  attempt2 INT DEFAULT NULL,  
  attempt_from_zone1 INT DEFAULT NULL,  
  goal_from_zone1 INT DEFAULT NULL,  
  attempt_from_zone2 INT DEFAULT NULL,  
  goal_from_zone2 INT DEFAULT NULL,  
  netPoints INT DEFAULT NULL,  

  -- Match Information
  shortDisplayName VARCHAR(45) DEFAULT NULL,
  firstname VARCHAR(45) DEFAULT NULL,
  surname VARCHAR(45) DEFAULT NULL,
  displayName VARCHAR(45) DEFAULT NULL,
  squadName VARCHAR(45) DEFAULT NULL,
  homeId VARCHAR(50) NOT NULL,
  awayId VARCHAR(50) NOT NULL,
  opponent VARCHAR(45) DEFAULT NULL,
  round INT DEFAULT NULL,
  fixtureId VARCHAR(50) NOT NULL,
  sportId VARCHAR(50) NOT NULL,
  matchId VARCHAR(50) NOT NULL,

  -- Additional Match Statistics
  turnoverHeld INT DEFAULT NULL,
  centrePassToGoalPerc INT DEFAULT NULL,
  deflectionWithNoGain INT DEFAULT NULL,
  generalPlayTurnovers INT DEFAULT NULL,
  interceptPassThrown INT DEFAULT NULL,
  points INT DEFAULT NULL,
  deflectionWithGain INT DEFAULT NULL,
  secondPhaseReceive INT DEFAULT NULL,
  deflectionPossessionGain INT DEFAULT NULL,
  possessionChanges INT DEFAULT NULL,
  feedWithAttempt INT DEFAULT NULL,
  unforcedTurnovers INT DEFAULT NULL,
  gainToGoalPerc INT DEFAULT NULL,

  -- Precomputed Columns (calculated in the application layer)
  uniqueFixtureId VARCHAR(255) NOT NULL,
  uniquePlayerId VARCHAR(255) NOT NULL,
  uniqueSquadId VARCHAR(255) NOT NULL,
  uniqueSportId VARCHAR(255) NOT NULL,
  uniqueMatchId VARCHAR(255) NOT NULL,

  -- Primary Key
  PRIMARY KEY (uniqueMatchId),

  -- Foreign Keys
  FOREIGN KEY (uniqueFixtureId) REFERENCES netball_womens_international_fixture(uniqueFixtureId),
  FOREIGN KEY (uniquePlayerId) REFERENCES player_info(uniquePlayerId),
  FOREIGN KEY (uniqueSquadId) REFERENCES squad_info(uniqueSquadId),
  FOREIGN KEY (uniqueSportId) REFERENCES sport_info(uniqueSportId)
);
