CREATE OR REPLACE TABLE BASE.LINES_TABLE AS 
WITH RAW_DATA AS (
             
            SELECT CAST(
            SUBSTRING(CAST(date AS VARCHAR), 1, 4) || '-' || 
            SUBSTRING(CAST(date AS VARCHAR), 5, 2) || '-' || 
            SUBSTRING(CAST(date AS VARCHAR), 7, 2)
            as DATE
            )
            AS P_DATE,
            
            team,
            line,
            total,
             
            from raw.lines_table
             )
             
            select
             lt.GAME_ID as gameId,
             lt.GAME_DATE,
             lt.TEAM_ABBREVIATION as teamTriCode,
             rd.line as LINE,
             rd.total as OU

             from RAW_DATA rd
             join raw.LINE_TEAM_MAPPING mp
                on mp.c1 = rd.team
             inner join raw.log_table lt
             on lt.GAME_DATE::DATE = rd.P_DATE
             and lt.TEAM_NAME = mp.c2
             
;