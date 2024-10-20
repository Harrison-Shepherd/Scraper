INSERT INTO afl_mens_match (
  sportId, 
  clearances, 
  tackles, 
  goals, 
  squadId, 
  displayName, 
  playerId, 
  marksContested, 
  behinds, 
  goalAssists, 
  round, 
  fixtureId, 
  firstname, 
  opponent, 
  positionName, 
  squadName, 
  surname, 
  marks, 
  inside50s, 
  kicksIneffective, 
  marksUncontested, 
  hitouts, 
  possessionsUncontested, 
  marksInside50, 
  penalty50sAgainst, 
  handballs, 
  disposalEfficiency, 
  awayId, 
  positionId, 
  jumperNumber, 
  freesFor, 
  hitoutsToAdvantage, 
  kicks, 
  possessionsContested, 
  kickEfficiency, 
  positionCode, 
  clangers, 
  shortDisplayName, 
  freesAgainst, 
  disposals, 
  blocks, 
  matchId, 
  homeId, 
  kicksEffective, 
  uniqueFixtureId, 
  uniquePlayerId, 
  uniqueSquadId, 
  uniqueSportId, 
  uniqueMatchId
) 
VALUES (
  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
);
