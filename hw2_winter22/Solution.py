from typing import List

import Utility.DBConnector
import Utility.DBConnector as Connector
from Utility.ReturnValue import ReturnValue
from Utility.Exceptions import DatabaseException
from Business.Match import Match
from Business.Player import Player
from Business.Stadium import Stadium
from psycopg2 import sql


###########################################################################
#                    Auxiliary functions                                  #
###########################################################################


def res_to_player(result_set: Connector.ResultSet) -> Player:
    return Player(playerID=result_set.rows[0][0])


def res_to_match(result_set: Connector.ResultSet) -> Match:
    return Match(
        matchID=result_set.rows[0][0],
        competition=result_set.rows[0][1],
        homeTeamID=result_set.rows[0][2],
        awayTeamID=result_set.rows[0][3]
    )


def res_to_stadium(result_set: Connector.ResultSet) -> Stadium:
    return Stadium(
        stadiumID=result_set.rows[0][0],
        capacity=result_set.rows[0][1],
        belongsTo=result_set.rows[0][2]
    )


def check_none(attribute):
    """A function to convert python's None to NULL"""
    return 'NULL' if not attribute else attribute


# TODO: change to_print and print_exceptions to False
def sql_query(query: str, to_print=False, to_commit=True, print_exceptions=True):
    """A wrapper function to all sql queries to avoid duplications"""
    query = query.replace("\n", "")
    conn = None
    # default values to return if no exception happened
    res_dict = {"ret_val": ReturnValue.OK,
                "row_effected": 0,
                "entries": Connector.ResultSet()}

    try:
        conn = Connector.DBConnector()
        row_effected, entries = conn.execute(query, printSchema=to_print)
        # TODO: should we always commit?
        # if to_commit:
        #     conn.commit()
    except (DatabaseException.ConnectionInvalid,
            DatabaseException.database_ini_ERROR,
            DatabaseException.UNKNOWN_ERROR) as e:
        if print_exceptions:
            print(e)
        res_dict["ret_val"] = ReturnValue.ERROR
    except (DatabaseException.CHECK_VIOLATION,
            DatabaseException.NOT_NULL_VIOLATION,
            DatabaseException.FOREIGN_KEY_VIOLATION) as e:
        if print_exceptions:
            print(e)
        res_dict["ret_val"] = ReturnValue.BAD_PARAMS
    # except  as e:
    #     if print_exceptions:
    #         print(e)
    #     res_dict["ret_val"] = ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION as e:
        if print_exceptions:
            print(e)
        res_dict["ret_val"] = ReturnValue.ALREADY_EXISTS
    except Exception as e:
        if print_exceptions:
            print(e)
        res_dict["ret_val"] = ReturnValue.ERROR
    else:
        res_dict["row_effected"] = row_effected
        res_dict["entries"] = entries
    finally:
        if conn:
            # TODO: check how to use rollback
            conn.close()
        return res_dict


###########################################################################
#                           HW functions                                  #
###########################################################################

#        CREATE TABLE BelongTo(
#            StadiumID INTEGER UNIQUE,
#            TeamID INTEGER UNIQUE,
#            FOREIGN KEY (StadiumID) REFERENCES Stadiums(StadiumID) ON DELETE CASCADE,
#            FOREIGN KEY (TeamID) REFERENCES Teams(TeamID) ON DELETE CASCADE,
#            PRIMARY KEY (StadiumID, TeamID));
#

def createTables():
    # TODO: maybe add VIEWS?
    # TODO: check how to create id's from separate tables unique
    # TODO: check if this is legal: Away INTEGER CHECK(Away <> Home)
    sql_query('''
        CREATE TABLE Teams(
            TeamID INTEGER NOT NULL UNIQUE CHECK(TeamID > 0),
            PRIMARY KEY (TeamID));
        
        CREATE TABLE Players(
            PlayerID INTEGER NOT NULL UNIQUE CHECK(PlayerID > 0),
            TeamID INTEGER,
            Age INTEGER NOT NULL CHECK(Age > 0),
            Height INTEGER NOT NULL CHECK(Height > 0),
            Preferred_foot TEXT NOT NULL CHECK(Preferred_foot='Right' OR Preferred_foot='Left'),
            FOREIGN KEY (TeamID) REFERENCES Teams(TeamID) ON DELETE CASCADE,
            PRIMARY KEY (PlayerID));
        
        CREATE TABLE Stadiums(
            StadiumID INTEGER NOT NULL UNIQUE CHECK(StadiumID > 0),
            Capacity INTEGER NOT NULL CHECK(Capacity > 0),
            BelongsTo INTEGER UNIQUE,
            FOREIGN KEY (BelongsTo) REFERENCES Teams(TeamID) ON DELETE CASCADE,
            PRIMARY KEY (StadiumID));
            
        CREATE TABLE Matches(
            MatchID INTEGER NOT NULL UNIQUE CHECK(MatchID > 0),
            Competition TEXT NOT NULL CHECK(Competition='International' OR Competition='Domestic'),
            Home INTEGER,
            Away INTEGER CHECK(Away <> Home),
            FOREIGN KEY (Home) REFERENCES Teams(TeamID) ON DELETE CASCADE,
            FOREIGN KEY (Away) REFERENCES Teams(TeamID) ON DELETE CASCADE,
            PRIMARY KEY (MatchID));
            
        CREATE TABLE Goals(
            MatchID INTEGER,
            PlayerID INTEGER,
            Goals INTEGER NOT NULL CHECK(Goals > 0),
            FOREIGN KEY (MatchID) REFERENCES Matches(MatchID) ON DELETE CASCADE,
            FOREIGN KEY (PlayerID) REFERENCES Players(PlayerID) ON DELETE CASCADE,
            PRIMARY KEY (MatchID, PlayerID));
    ''')


def clearTables():
    sql_query('''
        DELETE FROM Teams;
        DELETE FROM Players;
        DELETE FROM Stadiums;
    ''')


def dropTables():
    # TODO: check if DROP use is legal
    # TODO: why eilon used EXISTS and CASCADE
    sql_query('''
        DROP TABLE IF EXISTS Teams CASCADE;
        DROP TABLE IF EXISTS Players CASCADE;
        DROP TABLE IF EXISTS Stadiums CASCADE;
        DROP TABLE IF EXISTS Matches CASCADE;
        DROP TABLE IF EXISTS Goals CASCADE;
    ''')


def addTeam(teamID: int) -> ReturnValue:
    res_dict = sql_query(f'''
        INSERT INTO Teams VALUES({teamID});
    ''')
    return res_dict["ret_val"]


def addMatch(match: Match) -> ReturnValue:
    match_id = check_none(match.getMatchID())
    competition = check_none(match.getCompetition())
    home_id = check_none(match.getHomeTeamID())
    away_id = check_none(match.getAwayTeamID())

    res_dict = sql_query(f'''
        INSERT INTO Matches
        VALUES({match_id}, '{competition}', {home_id}, {away_id});
    ''')
    return res_dict["ret_val"]


def getMatchProfile(matchID: int) -> Match:
    res_dict = sql_query(f'''
        SELECT *gwe
        FROM Matches
        WHERE MatchID = {matchID};
    ''')
    if res_dict["ret_val"] != ReturnValue.OK:
        return Match.badMatch()
    else:
        return res_to_match(res_dict["entries"])


def deleteMatch(match: Match) -> ReturnValue:
    res_dict = sql_query(f'''
        DELETE FROM Matches
        WHERE MatchID = {check_none(match.getMatchID())};
    ''')
    if res_dict["rows_affected"] == 0:
        return ReturnValue.NOT_EXISTS
    return res_dict["ret_val"]


def addPlayer(player: Player) -> ReturnValue:
    player_id = check_none(player.getPlayerID())
    team_id = check_none(player.getTeamID())
    player_age = check_none(player.getAge())
    player_height = check_none(player.getHeight())
    preferred_foot = check_none(player.getFoot())

    res_dict = sql_query(f'''
        INSERT INTO Players
        VALUES({player_id}, {team_id}, {player_age}, {player_height}, '{preferred_foot}');
    ''')
    return res_dict["ret_val"]


def getPlayerProfile(playerID: int) -> Player:
    res_dict = sql_query(f'''
        SELECT *
        FROM Players
        WHERE PlayerID = {playerID};
    ''')
    if res_dict["ret_val"] != ReturnValue.OK or res_dict["rows_affected"] == 0:
        return Player.badPlayer()
    else:
        return res_to_player(res_dict["entries"])


def deletePlayer(player: Player) -> ReturnValue:
    res_dict = sql_query(f'''
        DELETE FROM Players
        WHERE MatchID = {check_none(player.getPlayerID())};
    ''')
    if res_dict["rows_affected"] == 0:
        return ReturnValue.NOT_EXISTS
    return res_dict["ret_val"]


def addStadium(stadium: Stadium) -> ReturnValue:
    stadiumId = check_none(stadium.getStadiumID())
    capacity = check_none(stadium.getCapacity())
    belongs_to = check_none(stadium.getBelongsTo())
    res_dict = sql_query(f'''
        INSERT INTO Stadiums
        VALUES({stadiumId}, {capacity}, {belongs_to});
    ''')
    return res_dict["ret_val"]


def getStadiumProfile(stadiumID: int) -> Stadium:
    res_dict = sql_query(f'''
        SELECT *
        FROM Stadiums
        WHERE StadiumID = {stadiumID};
    ''')
    if res_dict["ret_val"] != ReturnValue.OK:
        return Stadium.badStadium()
    else:
        return res_to_stadium(res_dict["entries"])


def deleteStadium(stadium: Stadium) -> ReturnValue:
    res_dict = sql_query(f'''
        DELETE FROM Stadiums
        WHERE StadiumID = {check_none(stadium.getStadiumID())};
    ''')
    if res_dict["rows_affected"] == 0:
        return ReturnValue.NOT_EXISTS
    return res_dict["ret_val"]


def playerScoredInMatch(match: Match, player: Player, amount: int) -> ReturnValue:
    match_id = check_none(match.getMatchID())
    player_id = check_none(player.getPlayerID())
    goal_amount = check_none(amount)

    res_dict = sql_query(f'''
          INSERT INTO Goals
          VALUES({match_id}, {player_id}, {goal_amount});
      ''')
    return res_dict["ret_val"]

# TODO: UPDATE instead of INSERT consequences: player may or may not have scored in the match prior to this action
def playerDidntScoreInMatch(match: Match, player: Player) -> ReturnValue:
    match_id = check_none(match.getMatchID())
    player_id = check_none(player.getPlayerID())

    res_dict = sql_query(f'''
        DELETE FROM Goals
        WHERE PlayerID ={player_id} AND MatchID = {match_id};
      ''')
    if res_dict["ret_val"] == ReturnValue.NOT_EXISTS:
        return ReturnValue.Ok
    return res_dict["ret_val"]


def matchInStadium(match: Match, stadium: Stadium, attendance: int) -> ReturnValue:
    pass


def matchNotInStadium(match: Match, stadium: Stadium) -> ReturnValue:
    pass


def averageAttendanceInStadium(stadiumID: int) -> float:
    pass


def stadiumTotalGoals(stadiumID: int) -> int:
    pass


def playerIsWinner(playerID: int, matchID: int) -> bool:
    pass


def getActiveTallTeams() -> List[int]:
    res_dict = sql_query(f'''
        SELECT TeamID
        FROM PLAYERS
        WHERE Height >= 190
        GROUP BY TeamID
        HAVING COUNT(TeamID) >= 2
        ORDER BY TeamID DESC
        LIMIT 5;
      ''')
    return res_dict["ret_val"]


def getActiveTallRichTeams() -> List[int]:
    pass


def popularTeams() -> List[int]:
    pass


def getMostAttractiveStadiums() -> List[int]:
    pass


def mostGoalsForTeam(teamID: int) -> List[int]:
    pass


def getClosePlayers(playerID: int) -> List[int]:
    pass
