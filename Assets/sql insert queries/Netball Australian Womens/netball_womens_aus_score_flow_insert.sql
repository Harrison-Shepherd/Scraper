INSERT INTO netball_womens_australia_score_flow (
  period, 
  periodSeconds, 
  distanceCode, 
  scorepoints, 
  scoreName, 
  positionCode, 
  squadId, 
  playerId, 
  matchId, 
  scoreFlowId, 
  uniquePlayerId, 
  uniqueMatchId
) 
VALUES (
  %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
);
