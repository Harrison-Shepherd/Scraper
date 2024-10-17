INSERT INTO afl_mens_score_flow (
  period, 
  periodSeconds, 
  scorepoints, 
  scoreName, 
  squadId, 
  playerId, 
  matchId, 
  scoreFlowId, 
  uniquePlayerId, 
  uniqueMatchId
) 
VALUES (
  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
);
