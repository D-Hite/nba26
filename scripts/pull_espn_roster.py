from espn_api.basketball import League
from pathlib import Path
import json
import os
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv())



OUT_PATH = Path("output/league_rosters.json")

def safe_get(obj, attr):
    return getattr(obj, attr, None)


def extract_player(p):
    """
    Extract ESPN player fields into a JSON-serializable dict.
    """

    return {
        "playerId": safe_get(p, "playerId"),
        "name": safe_get(p, "name"),
        "position": safe_get(p, "position"),
        "proTeam": safe_get(p, "proTeam"),

        # roster / acquisition info
        "acquisitionType": safe_get(p, "acquisitionType"),

        # health
        "injured": safe_get(p, "injured"),
        "injuryStatus": safe_get(p, "injuryStatus"),

        # rankings / points
        "avg_points": safe_get(p, "avg_points"),
        "total_points": safe_get(p, "total_points"),
        "projected_avg_points": safe_get(p, "projected_avg_points"),
        "projected_total_points": safe_get(p, "projected_total_points"),
        "posRank": safe_get(p, "posRank"),

        # advanced / complex fields
        "nine_cat_averages": safe_get(p, "nine_cat_averages"),

        # # These are objects → convert safely
        # "schedule": (
        #     [s.__dict__ for s in p.schedule]
        #     if safe_get(p, "schedule") is not None
        #     else None
        # ),

        # "stats": (
        #     {k: v for k, v in p.stats.items()}
        #     if safe_get(p, "stats") is not None
        #     else None
        # ),
    }

def fetch_espn_rosters(league):
    rosters = {}

    for team in league.teams:
        team_name = team.team_name
        players = []

        for p in team.roster:
            players.append(extract_player(p))

        rosters[team_name] = players

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_PATH.write_text(json.dumps(rosters, indent=2))
    print(f"✅ Wrote ESPN rosters to {OUT_PATH}")



def main():
    league = League(
        league_id=int(os.environ["ESPN_LEAGUE_ID"]),
        year=int(os.environ["ESPN_SEASON"]),
        espn_s2=os.environ["ESPN_S2"],
        swid=os.environ["ESPN_SWID"],
    )

    for team in league.teams:
        print(team.team_name)
    fetch_espn_rosters(league)

if __name__ == '__main__':
    main()