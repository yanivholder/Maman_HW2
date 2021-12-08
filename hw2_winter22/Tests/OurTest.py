import unittest
import Solution
from Utility.ReturnValue import ReturnValue
from Tests.abstractTest import AbstractTest
from Business.Match import Match
from Business.Stadium import Stadium
from Business.Player import Player


class TestOurs(AbstractTest):

    def test_basic(self) -> None:
        match_1 = Match(1, "Domestic", 1, 2)
        match_2 = Match(2, "Domestic", 2, 1)
        stadium_1 = Stadium(1, 55000, 1)
        stadium_2 = Stadium(2, 25000, 2)
        stadium_3 = Stadium(3, 60, None)
        stadium_4 = Stadium(4, 60, None)
        player_1 = Player(1, 1, 20, 185, "Left")
        player_2 = Player(2, 1, 20, 199, "Left")
        player_3 = Player(3, 1, 20, 253, "Right")

        # Added teams [1,2], Matches [1], Stadium [1]
        self.assertEqual(ReturnValue.OK, Solution.addTeam(1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addTeam(2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addMatch(match_1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addStadium(stadium_1), "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.addStadium(Stadium(1, 5000, 1)), "ID 1 already exists")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.addStadium(Stadium(2, 5000, 3)), "teamID 3 not exists")
        self.assertEqual(ReturnValue.OK, Solution.addStadium(stadium_3), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addStadium(stadium_4), "Should work")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.playerScoredInMatch(match_1, player_1, 3), "playerID 1 not exists")

        # Added players [1,2]
        self.assertEqual(ReturnValue.OK, Solution.addPlayer(player_1), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.addPlayer(player_2), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.playerScoredInMatch(match_1, player_1, 3), "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.playerScoredInMatch(match_1, player_1, 3), "Player Already scored")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.playerScoredInMatch(match_1, player_2, -3), "Player cannot score negative values")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.playerScoredInMatch(match_1, player_3, 3), "PlayerID 3 doesnt exist")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.playerScoredInMatch(match_2, player_2, 3), "MatchID 2 doesnt exist")
        self.assertEqual(ReturnValue.OK, Solution.playerScoredInMatch(match_1, player_2, 3), "Should work")
        self.assertEqual(ReturnValue.OK, Solution.playerDidntScoreInMatch(match_1, player_2), "Should work")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.playerDidntScoreInMatch(match_1, player_2), "Player has no records of scoring")
        self.assertEqual(ReturnValue.OK, Solution.playerScoredInMatch(match_1, player_2, 5), "Should work")

        # Match In stadium tests
        self.assertEqual(ReturnValue.OK, Solution.addStadium(stadium_2), "Should work")
        self.assertEqual(ReturnValue.BAD_PARAMS, Solution.matchInStadium(match_1, stadium_1, -25000), "Cannot have negative attended")
        self.assertEqual(ReturnValue.OK, Solution.matchInStadium(match_1, stadium_1, 25000), "Should work")
        self.assertEqual(ReturnValue.ALREADY_EXISTS, Solution.matchInStadium(match_1, stadium_1, 25000), "Match Already exists")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.matchNotInStadium(match_1, stadium_2), "Match not in stadium 2")
        self.assertEqual(ReturnValue.OK, Solution.matchNotInStadium(match_1, stadium_1), "Should work")
        self.assertEqual(ReturnValue.NOT_EXISTS, Solution.matchNotInStadium(match_1, stadium_1), "Match already removed and does not exist")


if __name__ == '__main__':
    unittest.main(verbosity=2, exit=False)
