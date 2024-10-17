CREATE TABLE squad_info (
  -- Squad Information
  squadId VARCHAR(50) NOT NULL,
  squadName VARCHAR(45) NOT NULL,
  
  -- Explicitly set uniqueSquadId
  uniqueSquadId VARCHAR(255) NOT NULL,

  -- Primary Key
  PRIMARY KEY (uniqueSquadId)
);
