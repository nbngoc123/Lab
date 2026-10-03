"""Payload mô phỏng, dựng từ chính dữ liệu mẫu trong log test API của người dùng."""
import copy

BAYERN = {"teamId": 40, "teamName": "FC Bayern München", "shortName": "Bayern", "teamIconUrl": "u40", "teamGroupName": None}
STUTT = {"teamId": 16, "teamName": "VfB Stuttgart", "shortName": "Stuttgart", "teamIconUrl": "u16", "teamGroupName": None}
BVB = {"teamId": 7, "teamName": "Borussia Dortmund", "shortName": "Dortmund", "teamIconUrl": "u7", "teamGroupName": None}
HSV = {"teamId": 100, "teamName": "Hamburger SV", "shortName": "HSV", "teamIconUrl": "u100", "teamGroupName": None}
BRE = {"teamId": 134, "teamName": "SV Werder Bremen", "shortName": "Bremen", "teamIconUrl": "u134", "teamGroupName": None}
G1 = {"groupName": "1. Spieltag", "groupOrderID": 1, "groupID": 50633}
G5 = {"groupName": "5. Spieltag", "groupOrderID": 5, "groupID": 50637}


def _goal(gid, s1, s2, minute, gid_getter, name, team, own=False, ot=False):
    return {"goalID": gid, "scoreTeam1": s1, "scoreTeam2": s2, "matchMinute": minute, "goalGetterID": gid_getter,
            "goalGetterName": name, "scoringTeamId": team, "isPenalty": False, "isOwnGoal": own,
            "isOvertime": ot, "comment": None}


def _res(rid, name, order, tid, kind, p1, p2):
    return {"resultID": rid, "resultName": name, "pointsTeam1": p1, "pointsTeam2": p2, "resultOrderID": order,
            "resultTypeID": tid, "resultTypeKind": kind, "resultDescription": "d"}


def match_bayern():   # Bayern 5-1 Stuttgart (có bàn phản lưới nhà + bàn bù giờ)
    return {"matchID": 83156, "matchDateTime": "2026-08-28T20:30:00", "timeZoneID": None, "leagueId": 4937,
            "leagueName": "1. Fußball-Bundesliga 2026/2027", "leagueSeason": 2026, "leagueShortcut": "bl1",
            "matchDateTimeUTC": "2026-08-28T18:30:00Z", "group": G1, "team1": BAYERN, "team2": STUTT,
            "lastUpdateDateTime": "2026-09-02T15:36:25.313", "matchIsFinished": True,
            "matchResults": [_res(127516, "Halbzeit", 1, 1, "HalfTime", 1, 0),
                             _res(127517, "Endergebnis", 2, 2, "After90Minutes", 5, 1)],
            "goals": [_goal(146249, 1, 0, 21, 19198, "D. Upamecano", 40),
                      _goal(146252, 1, 1, 52, 18488, "J. Vagnoman", 16),
                      _goal(146253, 2, 1, 55, 23135, "M. Olise", 40),
                      _goal(146254, 3, 1, 57, 18488, "J. Vagnoman", 40, own=True),
                      _goal(146257, 4, 1, 82, 24759, "Aleksandar Pavlovic", 40),
                      _goal(146259, 5, 1, 92, 18880, "Luis Díaz", 40, ot=True)],
            "location": None, "numberOfViewers": None}


def match_dortmund():  # Dortmund 2-0 HSV
    return {"matchID": 83157, "matchDateTime": "2026-08-29T18:30:00", "timeZoneID": None, "leagueId": 4937,
            "leagueName": "1. Fußball-Bundesliga 2026/2027", "leagueSeason": 2026, "leagueShortcut": "bl1",
            "matchDateTimeUTC": "2026-08-29T16:30:00Z", "group": G1, "team1": BVB, "team2": HSV,
            "lastUpdateDateTime": "2026-08-29T20:23:29.203", "matchIsFinished": True,
            "matchResults": [_res(127574, "Halbzeit", 1, 1, "HalfTime", 2, 0),
                             _res(127575, "Endergebnis", 2, 2, "After90Minutes", 2, 0)],
            "goals": [_goal(146350, 1, 0, 9, 19545, "S. Guirassy", 7),
                      _goal(146352, 2, 0, 46, 27282, "Giannis Konstantelias", 7, ot=True)],
            "location": {"locationID": 55, "locationCity": "Dortmund", "locationStadium": "Signal Iduna Park"},
            "numberOfViewers": 81365}


def match_future():    # Dortmund - Bremen chưa đá
    return {"matchID": 83192, "matchDateTime": "2026-10-09T20:30:00", "timeZoneID": "W. Europe Standard Time",
            "leagueId": 4937, "leagueName": "1. Fußball-Bundesliga 2026/2027", "leagueSeason": 2026,
            "leagueShortcut": "bl1", "matchDateTimeUTC": "2026-10-09T18:30:00Z", "group": G5, "team1": BVB,
            "team2": BRE, "lastUpdateDateTime": "2026-09-10T17:22:25.727", "matchIsFinished": False,
            "matchResults": [], "goals": [], "location": None, "numberOfViewers": None}


def season_payload():
    return [match_bayern(), match_dortmund(), match_future()]


LEAGUES_2026 = [{"leagueId": 4937, "leagueName": "1. Fußball-Bundesliga 2026/2027", "leagueShortcut": "bl1",
                 "leagueSeason": "2026", "sport": {"sportId": 1, "sportName": "Fußball"}},
                {"leagueId": 4897, "leagueName": "WM 2026", "leagueShortcut": "wm26", "leagueSeason": "2026",
                 "sport": {"sportId": 1, "sportName": "Fußball"}}]


class FakeClient:
    """Thay OpenLigaClient: trả payload dựng sẵn, đếm request."""
    def __init__(self):
        self.season = season_payload()
        self.requests_made = 0
        self.last_change_value = "2026-09-10T17:23:11.1"

    def _c(self): self.requests_made += 1
    def sports(self): self._c(); return [{"sportId": 1, "sportName": "Fußball"}]
    def result_types(self): self._c(); return [{"id": 1, "name": "Halbzeit", "kind": "HalfTime"},
                                              {"id": 2, "name": "Endergebnis", "kind": "After90Minutes"}]
    def leagues(self, season): self._c(); return copy.deepcopy(LEAGUES_2026) if season == 2026 else []
    def result_infos(self, lid):
        self._c()
        return [{"id": 6101, "name": "Halbzeit", "description": "x", "orderId": 1,
                 "globalResultInfo": {"id": 1, "name": "Halbzeit", "kind": "HalfTime"}}] if lid == 4937 else []
    def season_matches(self, sc, season):
        self._c(); return copy.deepcopy(self.season) if (sc, season) == ("bl1", 2026) else []
    def group_matches(self, sc, season, go):
        self._c(); return [copy.deepcopy(m) for m in self.season if m["group"]["groupOrderID"] == go]
    def last_change(self, sc, season, go): self._c(); return self.last_change_value
