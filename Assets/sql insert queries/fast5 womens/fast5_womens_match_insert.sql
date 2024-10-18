INSERT INTO fast5_womens_match (
  playerId, 
  matchId, 
  squadId, 
  firstname, 
  surname, 
  displayName, 
  shortDisplayName, 
  currentPositionCode, 
  startingPositionCode, 
  squadName, 
  homeId, 
  awayId, 
  opponent, 
  round, 
  fixtureId, 
  sportId, 
  powerPlayPeriod, 
  quartersPlayed, 
  minutesPlayed, 
  goals, 
  goals1, 
  goals2, 
  goals3, 
  goalAttempts, 
  goalAttempts1, 
  goalAttempts2, 
  goalAttempts3, 
  goalMisses, 
  goalMisses1, 
  goalMisses2, 
  goalMisses3, 
  points, 
  rebounds, 
  defensiveRebounds, 
  offensiveRebounds, 
  deflections, 
  deflectionWithGain, 
  deflectionWithNoGain, 
  deflectionPossessionGain, 
  intercepts, 
  interceptPassThrown, 
  gain, 
  gainToGoalPerc, 
  pickups, 
  blocked, 
  blocks, 
  turnovers, 
  generalPlayTurnovers, 
  missedGoalTurnover, 
  unforcedTurnovers, 
  turnoverHeld, 
  possessionChanges, 
  passes, 
  feeds, 
  feedWithAttempt, 
  goalAssists, 
  centrePassToGoalPerc, 
  centrePassReceives, 
  secondPhaseReceive, 
  penalties, 
  contactPenalties, 
  obstructionPenalties, 
  offsides, 
  badPasses, 
  badHands, 
  breaks, 
  tossUpWin, 
  uniqueFixtureId, 
  uniquePlayerId, 
  uniqueSquadId, 
  uniqueSportId, 
  uniqueMatchId
) 
VALUES (
  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
);
