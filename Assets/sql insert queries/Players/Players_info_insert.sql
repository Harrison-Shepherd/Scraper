INSERT INTO player_info (
  playerId, 
  firstname, 
  surname, 
  displayName, 
  shortDisplayName, 
  squadName, 
  squadId, 
  sportId, 
  uniqueSquadId, 
  uniquePlayerId
) 
VALUES (
  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
);
