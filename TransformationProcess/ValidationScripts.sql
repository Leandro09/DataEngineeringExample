SELECT
   DISTINCT "endReason"
FROM
  "callsData".calls_dbt_model;
  
  
 SELECT COUNT(*)
 FROM "callsData".calls_dbt_model c
 WHERE c.start like '%2022%';
 
 
 SELECT MIN(totalduration)
 FROM "callsData".calls_dbt_model;
 
 
 SELECT MAX(totalduration)
 FROM "callsData".calls_dbt_model;
 
 SELECT * FROM "callsData".calls_dbt_model c
 WHERE (c.ringing + c.connected + c.wrap) != c.totalduration