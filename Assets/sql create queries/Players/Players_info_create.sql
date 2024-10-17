CREATE TABLE player_info (
  -- Player Information
  playerId VARCHAR(50) NOT NULL,
  firstname VARCHAR(255) DEFAULT NULL,
  surname VARCHAR(255) DEFAULT NULL,  
  displayName VARCHAR(255) DEFAULT NULL,
  shortDisplayName VARCHAR(255) DEFAULT NULL,
  squadName VARCHAR(255) NOT NULL,
  
  -- Squad and Sport Information
  squadId VARCHAR(50) NOT NULL,
  sportId VARCHAR(50) DEFAULT NULL,  

  -- Precomputed uniqueSquadId and uniquePlayerId
  uniqueSquadId VARCHAR(255) NOT NULL,
  uniquePlayerId VARCHAR(255) NOT NULL,
  
  -- Primary Key
  PRIMARY KEY (uniquePlayerId),

  -- Foreign Key for squadId
  FOREIGN KEY (uniqueSquadId) REFERENCES squad_info(uniqueSquadId)
);
