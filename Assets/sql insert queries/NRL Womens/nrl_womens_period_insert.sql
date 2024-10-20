INSERT INTO NRL_womens_period (
  matchId, 
  squadId, 
  periodId, 
  playerId, 
  jumperNumber, 
  position, 
  tries, 
  tryAssists, 
  trySaves, 
  conversions, 
  conversionsUnsuccessful, 
  conversionAttempts, 
  penaltyGoals, 
  penaltyGoalAttempts, 
  fieldGoals, 
  fieldGoalAttempts, 
  runs, 
  runMetres, 
  metresGained, 
  runsNormal, 
  runsNormalMetres, 
  runsKickReturn, 
  runsKickReturnMetres, 
  runsHitup, 
  runsHitupMetres, 
  runsDummyHalf, 
  runsDummyHalfMetres, 
  postContactMetres, 
  tackles, 
  tackleds, 
  tackleBreaks, 
  tacklesIneffective, 
  missedTackles, 
  lineBreaks, 
  lineBreakAssists, 
  offloads, 
  kickMetres, 
  kicksGeneralPlay, 
  kicksCaught, 
  bombKicksCaught, 
  fortyTwenty, 
  handlingErrors, 
  penaltiesConceded, 
  errors, 
  passes, 
  goalLineDropouts, 
  sentOffs, 
  sinBins, 
  onReports, 
  ineffectiveTackles, 
  incompleteSets, 
  tacklesMissed, 
  scrumWins, 
  score, 
  uniqueMatchId, 
  uniquePlayerId, 
  uniquePeriodId,
  period
) 
VALUES (
  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
);
