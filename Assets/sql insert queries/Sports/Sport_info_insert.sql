INSERT INTO sport_info (
  sportId, 
  sportName, 
  fixtureId, 
  fixtureTitle, 
  fixtureYear, 
  uniqueSportId
) 
VALUES (
  %s, %s, %s, %s, %s, %s
);
