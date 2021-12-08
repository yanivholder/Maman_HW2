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


# TODO: change to_print to False
def sql_query(query, to_print=True):
    """A wrapper function to all sql queries to avoid duplications"""
    conn = None
    # default values to return if no exception happened
    ret_val = ReturnValue.OK
    row_effected, entries = 0, Connector.ResultSet()

    try:
        conn = Connector.DBConnector()
        row_effected, entries = conn.execute(query, printSchema=to_print)
        # TODO: should we always commit?
        conn.commit()
    except (DatabaseException.ConnectionInvalid,
            DatabaseException.database_ini_ERROR,
            DatabaseException.UNKNOWN_ERROR):
        return ReturnValue.ERROR
    except (DatabaseException.CHECK_VIOLATION,
            DatabaseException.NOT_NULL_VIOLATION):
        return ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        return ReturnValue.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION:
        return ReturnValue.ALREADY_EXISTS
    except:
        return ReturnValue.ERROR
    finally:
        if conn:
            # TODO: check how to use rollback
            conn.close()
        return ReturnValue.OK, row_effected, entries

###########################################################################
#                           HW functions                                  #
###########################################################################


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
            PRIMARY KEY (StadiumID));
        
        CREATE TABLE BelongTo(
            StadiumID INTEGER UNIQUE,
            TeamID INTEGER UNIQUE,
            FOREIGN KEY (StadiumID) REFERENCES Stadiums(StadiumID) ON DELETE CASCADE,
            FOREIGN KEY (TeamID) REFERENCES Teams(TeamID) ON DELETE CASCADE,
            PRIMARY KEY (StadiumID, TeamID));
            
        CREATE TABLE Match(
            MatchID INTEGER NOT NULL UNIQUE CHECK(MatchID > 0),
            Competition TEXT NOT NULL CHECK(Competition='International' OR Competition='Domestic'),
            Home INTEGER,
            Away INTEGER CHECK(Away <> Home),
            FOREIGN KEY (Home) REFERENCES Teams(TeamID) ON DELETE CASCADE,
            FOREIGN KEY (Away) REFERENCES Teams(TeamID) ON DELETE CASCADE,
            PRIMARY KEY (MatchID));
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
        DROP TABLE Teams;
        DROP TABLE Players;
        DROP TABLE Stadiums;
    ''')


def addTeam(teamID: int) -> ReturnValue:
    ret_val, _, _ = sql_query(f'''
        INSERT INTO Teams VALUES({teamID});
    ''')
    return ret_val


def addMatch(match: Match) -> ReturnValue:
    match_id = check_none(match.getMatchID())
    competition = check_none(match.getCompetition())
    home_id = check_none(match.getHomeTeamID())
    away_id = check_none(match.getAwayTeamID())

    ret_val, _, _ = sql_query(f'''
        INSERT INTO Matches
        VALUES({match_id}, {competition}, {home_id}, {away_id});
    ''')
    return ret_val


def getMatchProfile(matchID: int) -> Match:
    ret_val, _, entries = sql_query(f'''
            SELECT *
            FROM Matches
            WHERE MatchID = {matchID}
        ''')
    if ret_val != ReturnValue.OK:
        return Match.badMatch()
    else:
        return res_to_match(entries)


def deleteMatch(match: Match) -> ReturnValue:
    ret_val, row_effected, _ = sql_query(f'''
        DELETE FROM Matches
        WHERE MatchID = {check_none(match.getMatchID())}
    ''')
    if ret_val.rows_affected == 0:
        return ReturnValue.NOT_EXISTS
    return ret_val


def addPlayer(player: Player) -> ReturnValue:
    ret_val, _, _ = sql_query(f'''
            INSERT INTO Matches
            VALUES({check_none(player.getPlayerID())};
        ''')
    return ret_val


def getPlayerProfile(playerID: int) -> Player:
    ret_val, _, entries = sql_query(f'''
                SELECT *
                FROM Players
                WHERE PlayerID = {playerID}
            ''')
    if ret_val != ReturnValue.OK:
        return Player.badPlayer()
    else:
        return res_to_player(entries)


def deletePlayer(player: Player) -> ReturnValue:
    ret_val, row_effected, _ = sql_query(f'''
        DELETE FROM Players
        WHERE MatchID = {check_none(player.getPlayerID())}
    ''')
    if ret_val.rows_affected == 0:
        return ReturnValue.NOT_EXISTS
    return ret_val


def addStadium(stadium: Stadium) -> ReturnValue:
    ret_val = None
    stadiumId = check_none(stadium.getStadiumID())
    if stadium.getBelongsTo():
        ret_val, _, _ = sql_query(f'''
                INSERT INTO Matches
                VALUES({stadiumId});
                INSERT INTO BelongTo
                VALUES({stadiumId}, {check_none(stadium.getBelongsTo())};
            ''')
    else:
        ret_val, _, _ = sql_query(f'''
                INSERT INTO Stadiums
                INSERT
                VALUES({check_none(stadium.getPlayerID())};
            ''')
    return ret_val


def getStadiumProfile(stadiumID: int) -> Stadium:
    ret_val, _, entries = sql_query(f'''
                    SELECT *
                    FROM Stadiums
                    WHERE StadiumID = {stadiumID}
                ''')
    if ret_val != ReturnValue.OK:
        return Stadium.badStadium()
    else:
        return res_to_stadium(entries)


def deleteStadium(stadium: Stadium) -> ReturnValue:
    ret_val, row_effected, _ = sql_query(f'''
            DELETE FROM Stadiums
            WHERE StadiumID = {check_none(stadium.getStadiumID())}
        ''')
    if ret_val.rows_affected == 0:
        return ReturnValue.NOT_EXISTS
    return ret_val


def playerScoredInMatch(match: Match, player: Player, amount: int) -> ReturnValue:
    pass


def playerDidntScoreInMatch(match: Match, player: Player) -> ReturnValue:
    pass


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
    pass


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
