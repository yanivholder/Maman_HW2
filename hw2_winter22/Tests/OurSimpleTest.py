from Solution import *
from Utility.ReturnValue import ReturnValue
from Tests.abstractTest import AbstractTest
from Business.Match import Match
from Business.Stadium import Stadium
from Business.Player import Player


if __name__ == '__main__':
    dropTables()
    createTables()
    team_a = 13
    assert addTeam(team_a) == ReturnValue.OK
    yosi = Player(1, team_a, 10, 10, 'Right')
    assert addPlayer(yosi) == ReturnValue.OK
    yosi2 = getPlayerProfile(1)
    assert yosi.getPlayerID() == yosi2.getPlayerID()

    dropTables()
