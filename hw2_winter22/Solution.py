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
def sql_query(query: str, to_print=False, to_commit=True, print_exceptions=True, is_add_func=False,
              is_delete_func=False):
    """A wrapper function to all sql queries to avoid duplications"""
    query = query.replace("\n", "")
    conn = None
    # default values to return if no exception happened
    res_dict = {"ret_val": ReturnValue.OK,
                "row_effected": -1,
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
            DatabaseException.NOT_NULL_VIOLATION) as e:
        if print_exceptions:
            print(e)
        res_dict["ret_val"] = ReturnValue.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        if print_exceptions:
            print(e)
        res_dict["ret_val"] = ReturnValue.NOT_EXISTS
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
        if is_add_func:
            if res_dict["ret_val"] == ReturnValue.NOT_EXISTS:
                res_dict["ret_val"] = ReturnValue.BAD_PARAMS
        if is_delete_func:
            if res_dict["row_effected"] == 0:
                res_dict["ret_val"] = ReturnValue.NOT_EXISTS
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
            
        CREATE TABLE ScoredIn(
            MatchID INTEGER,
            PlayerID INTEGER,
            Goals INTEGER NOT NULL CHECK(Goals > 0),
            FOREIGN KEY (MatchID) REFERENCES Matches(MatchID) ON DELETE CASCADE,
            FOREIGN KEY (PlayerID) REFERENCES Players(PlayerID) ON DELETE CASCADE,
            PRIMARY KEY (MatchID, PlayerID));
            
        CREATE TABLE MatchIn(
            MatchID INTEGER,
            StadiumID INTEGER,
            Attended INTEGER NOT NULL CHECK(Attended >= 0),
            FOREIGN KEY (MatchID) REFERENCES Matches(MatchID) ON DELETE CASCADE,
            FOREIGN KEY (StadiumID) REFERENCES Stadiums(STADIUMID) ON DELETE CASCADE,
            PRIMARY KEY (MatchID));
        
        CREATE VIEW MatchAndTotalGoals AS
            SELECT MatchID, SUM(Goals) AS TotalGoals
            FROM ScoredIn
            GROUP BY MatchID;
        
        CREATE VIEW ActiveTallTeams AS 
            SELECT TeamID
            FROM Players, Matches
            WHERE Height > 190 AND (TeamID = Home OR TeamID = Away)
            GROUP BY TeamID
            HAVING COUNT(TeamID) >= 2;
            
        CREATE VIEW RichTeams AS 
            SELECT BelongsTo AS TeamID
            FROM Stadiums
            WHERE Capacity > 55000;
            
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
        DROP TABLE IF EXISTS ScoredIn CASCADE;
        DROP TABLE IF EXISTS MatchIn CASCADE;
        DROP TABLE IF EXISTS MatchAndTotalGoals;
        DROP TABLE IF EXISTS ActiveTallTeams;
        DROP TABLE IF EXISTS RichTeams;
    ''')


def addTeam(teamID: int) -> ReturnValue:
    res_dict = sql_query(f'''
        INSERT INTO Teams VALUES({teamID});
    ''', is_add_func=True)

    return res_dict["ret_val"]


def addMatch(match: Match) -> ReturnValue:
    match_id = check_none(match.getMatchID())
    competition = check_none(match.getCompetition())
    home_id = check_none(match.getHomeTeamID())
    away_id = check_none(match.getAwayTeamID())

    res_dict = sql_query(f'''
        INSERT INTO Matches
        VALUES({match_id}, '{competition}', {home_id}, {away_id});
    ''', is_add_func=True)
    return res_dict["ret_val"]


def getMatchProfile(matchID: int) -> Match:
    res_dict = sql_query(f'''
        SELECT *
        FROM Matches
        WHERE MatchID = {matchID};
    ''')
    if res_dict["ret_val"] != ReturnValue.OK or res_dict["row_effected"] == 0:
        return Match.badMatch()
    else:
        return res_to_match(res_dict["entries"])


def deleteMatch(match: Match) -> ReturnValue:
    res_dict = sql_query(f'''
        DELETE FROM Matches
        WHERE MatchID = {check_none(match.getMatchID())};
    ''', is_delete_func=True)

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
    ''', is_add_func=True)
    return res_dict["ret_val"]


def getPlayerProfile(playerID: int) -> Player:
    res_dict = sql_query(f'''
        SELECT *
        FROM Players
        WHERE PlayerID = {playerID};
    ''')
    if res_dict["ret_val"] != ReturnValue.OK or res_dict["row_effected"] == 0:
        return Player.badPlayer()
    else:
        return res_to_player(res_dict["entries"])


def deletePlayer(player: Player) -> ReturnValue:
    res_dict = sql_query(f'''
        DELETE FROM Players
        WHERE MatchID = {check_none(player.getPlayerID())};
    ''', is_delete_func=True)

    return res_dict["ret_val"]


def addStadium(stadium: Stadium) -> ReturnValue:
    stadiumId = check_none(stadium.getStadiumID())
    capacity = check_none(stadium.getCapacity())
    belongs_to = check_none(stadium.getBelongsTo())
    res_dict = sql_query(f'''
        INSERT INTO Stadiums
        VALUES({stadiumId}, {capacity}, {belongs_to});
    ''', is_add_func=True)
    return res_dict["ret_val"]


def getStadiumProfile(stadiumID: int) -> Stadium:
    res_dict = sql_query(f'''
        SELECT *
        FROM Stadiums
        WHERE StadiumID = {stadiumID};
    ''')
    if res_dict["ret_val"] != ReturnValue.OK or res_dict["row_effected"] == 0:
        return Stadium.badStadium()
    else:
        return res_to_stadium(res_dict["entries"])


def deleteStadium(stadium: Stadium) -> ReturnValue:
    res_dict = sql_query(f'''
        DELETE FROM Stadiums
        WHERE StadiumID = {check_none(stadium.getStadiumID())};
    ''', is_delete_func=True)

    return res_dict["ret_val"]


def playerScoredInMatch(match: Match, player: Player, amount: int) -> ReturnValue:
    match_id = check_none(match.getMatchID())
    player_id = check_none(player.getPlayerID())

    res_dict = sql_query(f'''
        INSERT INTO ScoredIn
        VALUES({match_id}, {player_id}, {amount});
    ''')
    return res_dict["ret_val"]


def playerDidntScoreInMatch(match: Match, player: Player) -> ReturnValue:
    match_id = check_none(match.getMatchID())
    player_id = check_none(player.getPlayerID())

    res_dict = sql_query(f'''
        DELETE FROM ScoredIn
        WHERE PlayerID = {player_id} AND MatchID = {match_id};
    ''', is_delete_func=True)

    return res_dict["ret_val"]


def matchInStadium(match: Match, stadium: Stadium, attendance: int) -> ReturnValue:
    match_id = check_none(match.getMatchID())
    stadium_id = check_none(stadium.getStadiumID())

    res_dict = sql_query(f'''
        INSERT INTO MatchIn
        VALUES({match_id}, {stadium_id}, {attendance});
    ''')

    return res_dict["ret_val"]


def matchNotInStadium(match: Match, stadium: Stadium) -> ReturnValue:
    match_id = check_none(match.getMatchID())
    stadium_id = check_none(stadium.getStadiumID())

    res_dict = sql_query(f'''
        DELETE FROM MatchIn
        WHERE MatchID = {match_id} AND StadiumID = {stadium_id};
    ''', is_delete_func=True)

    return res_dict["ret_val"]


def averageAttendanceInStadium(stadiumID: int) -> float:
    res_dict = sql_query(f'''
        SELECT AVG(Attended)
        FROM MatchIn
        WHERE StadiumID = {stadiumID};
    ''')

    # TODO: check the 0 division edge case
    # TODO: check the first condition works
    if not res_dict["entries"]:
        return 0
    elif res_dict["ret_val"] != ReturnValue.OK:
        return -1
    else:
        return res_dict["entries"].rows[0][0]


def stadiumTotalGoals(stadiumID: int) -> int:
    res_dict = sql_query(f'''
        SELECT SUM(Goals)
        FROM ScoredIn
        WHERE StadiumID = {stadiumID};
    ''')

    # TODO: check the first condition works
    if not res_dict["entries"]:
        return 0
    elif res_dict["ret_val"] != ReturnValue.OK:
        return -1
    else:
        return res_dict["entries"].rows[0][0]


def playerIsWinner(playerID: int, matchID: int) -> bool:
    res_dict = sql_query(f'''
        SELECT S.PlayerID
        FROM MatchAndTotalGoals INNER JOIN ScoredIn
        WHERE MatchID = {matchID} AND PlayerID = {playerID} AND Goals >= (2 * TotalGoals);
    ''')

    if res_dict["entries"]:
        return True
    else:
        return False


def getActiveTallTeams() -> List[int]:
    res_dict = sql_query(f'''
        SELECT * FROM ActiveTallTeams
        ORDER BY TeamID DESC
        LIMIT 5;
    ''')

    res = []
    try:
        for i in range(5):
            res.append(res_dict["entries"][i][0])
    finally:
        return res


def getActiveTallRichTeams() -> List[int]:
    res_dict = sql_query(f'''
        SELECT * 
        FROM ActiveTallTeams INTERSECT RichTeams
        ORDER BY TeamID ASC
        LIMIT 5;
    ''')

    res = []
    try:
        for i in range(5):
            res.append(res_dict["entries"][i][0])
    finally:
        return res


def popularTeams() -> List[int]:
    res_dict = sql_query(f'''
        SELECT TeamID
        FROM (SELECT TeamID FROM ((MatchIn INNER JOIN Matches), Teams) WHERE TeamID = Home)
        EXCEPT (SELECT TeamID FROM ((MatchIn UNION Matches), Teams) WHERE TeamID = Home AND Attended <= 40000)
        
        (SELECT TeamID                                                                                      , MatchID FROM (Matches LEFT OUTER JOIN MatchIn), Teams WHERE TeamID = Home)
        EXCEPT
        (SELECT Home AS TeamID FROM Matches LEFT OUTER JOIN MatchIn WHERE Attended <= 40000 OR Attended = NULL)
        
        ORDER BY TeamID ASC
        LIMIT 5);
    ''')

    res = []
    try:
        for i in range(10):
            res.append(res_dict["entries"][i][0])
    finally:
        return res


def getMostAttractiveStadiums() -> List[int]:
    res_dict = sql_query(f'''
        SELECT StadiumID, COUNT(TotalGoals) AS StadiumGoals
        FROM MatchAndTotalGoals INNER JOIN MatchIn
        GROUP BY StadiumID
        ORDER BY StadiumGoals DESC, StadiumID ASC
      ''')
    entries = res_dict["entries"]
    ret_val = []
    try:
        for i in range(0, len(entries)):
            ret_val.append(entries[i][0])
    finally:
        return ret_val
    pass


def mostGoalsForTeam(teamID: int) -> List[int]:
    res_dict = sql_query(f'''
        SELECT PlayerID, COUNT(Goals)
        FROM ScoredIn
        WHERE TeamID = {teamID}
        GROUP BY PlayerID
        ORDER BY Goals DESC, PlayerID DESC
        LIMIT 5;
      ''')
    entries = res_dict["entries"]
    ret_val = []
    try:
        for i in range(0, 5):
            ret_val.append(entries[i][0])
    finally:
        return ret_val
    pass


def getClosePlayers(playerID: int) -> List[int]:
    res_dict = sql_query(f'''
        SELECT PlayerID
        FROM    (SELECT PlayerID, COALESCE(COUNT(*), 0) AS ScoredWithPlayer
                 FROM ScoredIn AS A, ScoredIn AS B
                 WHERE B.PlayerID = {playerID} AND A.MatchID = B.MatchID
                 GROUP BY PlayerID) AS SharedMatches 
        WHERE PlayerID != {playerID} AND ScoredWithPlayer >=  0.5*(SELECT COUNT(*) 
                                                                FROM SharedMatches
                                                                WHERE PlayerID = {playerID}
                                                                )
        ORDER BY PlayerID ASC
        LIMIT 10;
      ''')
    entries = res_dict["entries"]
    ret_val = []
    try:
        for i in range(0, 5):
            ret_val.append(entries[i][0])
    finally:
        return ret_val
    pass
