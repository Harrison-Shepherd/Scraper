CREATE TABLE sport_info (
  -- Sport Information
  sportId VARCHAR(50) NOT NULL,
  sportName VARCHAR(255) DEFAULT NULL,
  
  -- Fixture Information
  fixtureId VARCHAR(50) NOT NULL,
  fixtureTitle VARCHAR(255) DEFAULT NULL,
  fixtureYear VARCHAR(50) DEFAULT NULL,
  
  -- Explicitly set uniqueSportId
  uniqueSportId VARCHAR(255) NOT NULL,

  -- Primary Key
  PRIMARY KEY (uniqueSportId)
);
