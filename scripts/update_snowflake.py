from snowflake.connector import connect
from snowflake.connector.pandas_tools import write_pandas
import os
import nba_api.stats.endpoints as ep
import pandas as pd
from datetime import datetime
from utils import logger

base_path = os.getcwd()

print(base_path)

def get_snowflake_conn():
    conn = connect(
        account="RPOSWFZ-OIB57673",
        user="grunk",
        private_key_file=os.path.join(base_path,'rsa_key.p8'),
        warehouse="compute_wh",
        database="NBA",
        schema="RAW"
    )

    return conn


## helper
def get_nba_season(today=None):
    if today is None:
        today = datetime.today()
    year = today.year
    if today.month < 7:
        start_year = year - 1
        end_year = year
    else:
        start_year = year
        end_year = year + 1

    return f"{start_year}-{str(end_year)[-2:]}"

# print(get_nba_season()) 

season = get_nba_season()

ENDPOINTS = ['advanced','fourfactors','misc','scoring','traditional']

FD = {
            'advanced': ep.boxscoreadvancedv3.BoxScoreAdvancedV3,
            'fourfactors': ep.boxscorefourfactorsv3.BoxScoreFourFactorsV3,
            'misc': ep.boxscoremiscv3.BoxScoreMiscV3,
            'scoring': ep.boxscorescoringv3.BoxScoreScoringV3,
            'traditional': ep.boxscoretraditionalv3.BoxScoreTraditionalV3
        }

TOP = ['teams','players']


def fetch_log():
    season = get_nba_season()
    gidset = set()
    log = None
    try:
        
    # fetch games for a specific season (reason to separate 002 and 004 is becuase there are other games (non nba games) included in the GameFinder ep)
        result = ep.leaguegamefinder.LeagueGameFinder(season_nullable=season)
        all_games = result.get_data_frames()[0]
        rs = all_games[all_games.SEASON_ID == '2' + season[:4]]
        rs = rs[rs.GAME_ID.str[:3] == '002']  # regular season
        os = all_games[all_games.SEASON_ID == '4' + season[:4]]
        os = os[os.GAME_ID.str[:3] == '004']  # postseason
        nba_cup = all_games[all_games.SEASON_ID == '6' + season[:4]]
        nba_cup = nba_cup[nba_cup.GAME_ID.str[:3] == '006']  # nba cup
        log = pd.concat([rs, os,nba_cup])
        gidset.update(log['GAME_ID'].apply(lambda x: x.zfill(10)))  # Normalize game_id with leading zeros
    except Exception as e:
        logger.error(f"FAILED TO FETCH LOG FOR SEASON, attempting to read local log {season} {e}")
        log_path = os.path.join(base_path,'data','raw','log',f'log{season}.csv')
        log = pd.read_csv(log_path, dtype={'GAME_ID': str})
        gidset.update(log['GAME_ID'].apply(lambda x: x.zfill(10)))

    return log,gidset

def write_data(endpoint_name,season, tstats, pstats, snowflake=False):
    try:
        # Check if files already exist, and if they do, append the data
        team_file = os.path.join(base_path,'data','raw','teams',endpoint_name,f'teams_{endpoint_name}{season}.csv')
        player_file = os.path.join(base_path,'data','raw','players',endpoint_name,f'players_{endpoint_name}{season}.csv')
        ## ensure location exists
        os.makedirs(os.path.dirname(team_file), exist_ok=True)
        os.makedirs(os.path.dirname(player_file), exist_ok=True)

        logger.info(f"attempting to write {len(tstats) / 2} games")

        # Append team if files exist, otherwise create new
        mode = 'a' if os.path.exists(team_file) else 'w'
        header = not os.path.exists(team_file)
        if mode == 'w':
            filtered_tstats = tstats
        else:
            current_tstats = pd.read_csv(team_file, usecols=['gameId','teamId'], dtype={'gameId': str,'teamId':int})
            filtered_tstats = tstats[~tstats[['gameId', 'teamId']].apply(tuple, 1).isin(current_tstats[['gameId', 'teamId']].apply(tuple, 1))]
        filtered_tstats.to_csv(team_file, mode=mode, header=header, index=False)

        # Append player if files exist, otherwise create new
        mode = 'a' if os.path.exists(player_file) else 'w'
        header = not os.path.exists(player_file)
        if mode == 'w':
            filtered_pstats = pstats
        else:
            current_pstats = pd.read_csv(player_file, usecols=['gameId','teamId','personId'], dtype={'gameId': str,'teamId':int,'personId':int})
            filtered_pstats = pstats[~pstats[['gameId', 'teamId','personId']].apply(tuple, 1).isin(current_pstats[['gameId', 'teamId','personId']].apply(tuple, 1))]
        filtered_pstats.to_csv(player_file, mode=mode, header=header, index=False)
        
        logger.info(f"Data written for {endpoint_name} - {season}")

        if snowflake:
            #TODO and test
            tstats.columns = [c.upper() for c in tstats.columns]
            pstats.columns = [c.upper() for c in pstats.columns]



            # write_pandas(conn, df, table_name, schema=None, database=None, chunk_size=16000, quote_identifiers=True)
            success, nchunks, nrows, _ = write_pandas(conn, tstats, f"TEAMS_{endpoint_name.upper()}")
            logger.info(f"for {endpoint_name} for {season} uploaded {len(tstats) / 2} games, {nrows} rows to snowflake with {success}")
            success, nchunks, nrows, _ = write_pandas(conn, pstats, f"PLAYERS_{endpoint_name.upper()}")
            logger.info(f"for {endpoint_name} for {season} uploaded {len(pstats)} games, {nrows} rows to snowflake with {success}")

            return


            ### maybe input filtered stats?
            # try:
            #     write_to_duckdb(endpoint_name,tstats,pstats)
            # except Exception as e:
            #     logger.error(f"problem with  write_to_duckdb{endpoint_name} - {season}, ERROR: {e}")


    except Exception as e:
        logger.error(f"UNABLE TO WRITE DATA FOR {endpoint_name} - {season}, ERROR: {e}")

import random
from requests.exceptions import Timeout
from urllib3.exceptions import ReadTimeoutError
import time

def get_game(func, gid):
    max_retries = 4
    retries = 0

    while retries < max_retries:
        try:
            # Attempt to fetch the game data
            game = func(game_id=gid).get_data_frames()
            players = game[0]
            teams = game[1]
            return players, teams

        except (TimeoutError, Timeout, ReadTimeoutError) as e:
            retries += 1
            delay = random.uniform(1, 3) * (2 ** retries)  # Exponential backoff
            error_message = f"Timeout error for {gid} (Attempt {retries}/{max_retries}). Retrying in {delay:.2f}s..."
            # print(error_message)
            # logger.warning(error_message)  # Log warning with retry info
            time.sleep(delay)

        except Exception as e:
            # Any other errors are treated as permanent failure for this game
            error_message = f"Error fetching game data for {gid}: {e}. Skipping this game."
            # print(error_message)
            logger.error(error_message)  # Log the error
            return None, None  # Skip the game on failure

    # If retries exceeded, skip the game and log the failure
    error_message = f"Failed to fetch game data for {gid} after {max_retries} attempts. Skipping this game."
    # print(error_message)
    logger.error(error_message)  # Log the final failure
    return None, None  # Skip this game if max retries exhausted





    
def get_gid_list(conn):
    log,current_season_games = fetch_log()
    # print(current_season_games)
    season_id = log['SEASON_ID'][0]
    print(season_id)

    ## UPDATE LOG TABLE IN SNOWFLAKE
    with conn.cursor() as cur:
        cur.execute(f"""SELECT DISTINCT GAME_ID FROM RAW.LOG_TABLE""")
        results = cur.fetchall()
        log_games = set(item[0] for item in results)

        filtered_log = log[~log['GAME_ID'].isin(log_games)]

        if not filtered_log.empty:
            success, nchunks, nrows, _ = write_pandas(conn, filtered_log, f"LOG_TABLE")


    ### TODO get games and upload to both csv files and to snowflake

    # for func in FD:
    for func in ['traditional']:
        with conn.cursor() as cur:
            cur.execute(f"""SELECT DISTINCT t1.GAMEID FROM RAW.TEAMS_{func.upper()} T1 join RAW.LOG_TABLE t2 on t1.GAMEID = t2.GAME_ID WHERE t2.SEASON_ID = {int(season_id)}""")
            results = cur.fetchall()
            stored_games = set(item[0] for item in results)
            # print(f"team_games = {len(stored_games)}")
            cur.execute(f"""SELECT DISTINCT t1.GAMEID FROM RAW.PLAYERS_{func.upper()} T1 join RAW.LOG_TABLE t2 on t1.GAMEID = t2.GAME_ID WHERE t2.SEASON_ID = {int(season_id)}""")
            results = cur.fetchall()
            # print(f"team_games = {len(set(item[0] for item in results))}")
            stored_games = stored_games.intersection(set(item[0] for item in results))
            # print(f"intersection = {len(stored_games)}")
            # print(stored_games)
        needed_games = current_season_games - stored_games
        message = f"getting {len(needed_games)} for {func}"
        logger.info(message)
        # break
        # print(needed_games)
        player_games = []
        team_games = []
        for gid in needed_games:
            res = get_game(FD[func],gid)
            # print(f"got game {gid}")
            if res:
                player_df = res[0]
                team_df = res[1]
                write_data(func,get_nba_season(),team_df, player_df, True)

                # player_games.append(player_df)
                # team_games.append(team_df)
            else:
                logger.info(f"missed {gid}")
            # break
        
        # write_data(func,get_nba_season(),pd.concat(team_games), pd.concat(player_games), True)

        


        


    
    # write_pandas(conn, df, table_name, schema=None, database=None, chunk_size=16000, quote_identifiers=True)
    # success, nchunks, nrows, _ = write_pandas(conn, df, 'GAMES')

    return


def get_gid_list_all_games(cur):

    return

if __name__ == "__main__":
    conn = get_snowflake_conn()
    get_gid_list(conn)
    conn.close()
