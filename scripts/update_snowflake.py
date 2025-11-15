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
        logger.log_error(f"FAILED TO FETCH LOG FOR SEASON, attempting to read local log {self.season} {e}")
        log_path = os.path.join(base_path,'data','raw','log',f'log{season}.csv')
        log = pd.read_csv(log_path, dtype={'GAME_ID': str})
        gidset.update(log['GAME_ID'].apply(lambda x: x.zfill(10)))

    return log,gidset


def get_gid_list(conn):
    log,current_season_games = fetch_log()
    # print(current_season_games)
    season_id = log['SEASON_ID'][0]
    print(season_id)

    with conn.cursor() as cur:
        cur.execute("SELECT MAX(LAST_UPDATED_TIMETAMP) AS latest_timestamp FROM EVENT_LOG.RAW_UPDATE_TABLE")
        result = cur.fetchall()
        latest_timestamp = result[0] if result else None
        if latest_timestamp:
            ### TODO make this filter on timestamp
            cur.execute(f"SELECT DISTINCT GAME_ID FROM RAW.LOG_TABLE WHERE SEASON_ID = {int(season_id)-1}")
            results = cur.fetchall()
            stored_games = set(item[0] for item in results)
            print(stored_games)
    needed_games = current_season_games - stored_games

    ### TODO get games and upload to both csv files and to snowflake


    
    # write_pandas(conn, df, table_name, schema=None, database=None, chunk_size=16000, quote_identifiers=True)
    # success, nchunks, nrows, _ = write_pandas(conn, df, 'GAMES')

    return


def get_gid_list_all_games(cur):

    return

if __name__ == "__main__":
    conn = get_snowflake_conn()
    get_gid_list(conn)
    conn.close()
