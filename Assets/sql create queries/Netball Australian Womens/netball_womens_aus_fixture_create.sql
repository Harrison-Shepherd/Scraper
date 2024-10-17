CREATE TABLE netball_womens_australia_fixture (
  -- Fixture Information
  fixtureId VARCHAR(50) NOT NULL,
  matchId VARCHAR(50) NOT NULL,
  matchNumber INT DEFAULT NULL,
  matchType VARCHAR(45) DEFAULT NULL,
  matchStatus VARCHAR(45) DEFAULT NULL,
  sportId VARCHAR(50) NOT NULL,

  -- Time Information
  periodSecs INT DEFAULT NULL,
  period INT DEFAULT NULL,
  periodCompleted INT DEFAULT NULL,
  localStartTime VARCHAR(45) DEFAULT NULL,
  utcStartTime VARCHAR(45) DEFAULT NULL,

  -- Home Squad Information
  homeSquadId VARCHAR(50) NOT NULL,
  homeSquadName VARCHAR(45) DEFAULT NULL,
  homeSquadShortCode VARCHAR(45) DEFAULT NULL,
  homeSquadNickname VARCHAR(45) DEFAULT NULL,
  homeSquadScore INT DEFAULT NULL,
  homeSquadCode VARCHAR(45) DEFAULT NULL,

  -- Away Squad Information
  awaySquadId VARCHAR(50) NOT NULL,
  awaySquadName VARCHAR(45) DEFAULT NULL,
  awaySquadNickname VARCHAR(45) DEFAULT NULL,
  awaySquadScore INT DEFAULT NULL,
  awaySquadCode VARCHAR(45) DEFAULT NULL,
  awaySquadShortCode VARCHAR(45) DEFAULT NULL,

  -- Venue Information
  venueId VARCHAR(50) NOT NULL,
  venueCode VARCHAR(45) DEFAULT NULL,
  venueName VARCHAR(45) DEFAULT NULL,

  -- Round and Final Information
  roundNumber INT DEFAULT NULL,
  finalCode VARCHAR(45) DEFAULT NULL,
  finalShortCode VARCHAR(45) DEFAULT NULL,

  -- Precomputed Columns (calculated in the application layer)
  matchName VARCHAR(255) NOT NULL,
  uniqueAwaySquadId VARCHAR(255) NOT NULL,
  uniqueHomeSquadId VARCHAR(255) NOT NULL,
  uniqueSportId VARCHAR(255) NOT NULL,
  uniqueFixtureId VARCHAR(255) NOT NULL,

  -- Primary Key
  PRIMARY KEY (uniqueFixtureId),

  -- Foreign Keys
  FOREIGN KEY (uniqueHomeSquadId) REFERENCES squad_info(uniqueSquadId),
  FOREIGN KEY (uniqueAwaySquadId) REFERENCES squad_info(uniqueSquadId),
  FOREIGN KEY (uniqueSportId) REFERENCES sport_info(uniqueSportId)
);
