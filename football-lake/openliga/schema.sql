-- ============================================================================
-- football_source : bản mirror có kiểm soát của OpenLigaDB (mùa 2024 -> nay)
-- Nguyên tắc: giữ nguyên ID của API làm PRIMARY KEY để upsert idempotent và
-- Debezium có key ổn định. File này idempotent (chạy lại nhiều lần an toàn).
-- ============================================================================

CREATE TABLE IF NOT EXISTS sports (
  sport_id     INT PRIMARY KEY,
  sport_name   TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS result_types (          -- /getresulttypes (global)
  result_type_id INT PRIMARY KEY,
  name           TEXT,
  kind           TEXT                              -- HalfTime/After90Minutes/AfterExtraTime/AfterPenalties/Unknown
);

CREATE TABLE IF NOT EXISTS leagues (               -- /getavailableleagues/{season}
  league_id    INT PRIMARY KEY,
  shortcut     TEXT NOT NULL,                      -- bl1, bl2, wm26 ...
  season       INT  NOT NULL,
  name         TEXT NOT NULL,
  sport_id     INT REFERENCES sports
);
CREATE INDEX IF NOT EXISTS ix_leagues_shortcut_season ON leagues (shortcut, season);

CREATE TABLE IF NOT EXISTS league_result_infos (   -- /getresultinfos/{leagueId}
  info_id          INT PRIMARY KEY,
  league_id        INT NOT NULL REFERENCES leagues ON DELETE CASCADE,
  name             TEXT,
  description      TEXT,
  order_id         INT,
  result_type_id   INT,
  result_type_kind TEXT
);

CREATE TABLE IF NOT EXISTS teams (
  team_id      INT PRIMARY KEY,
  team_name    TEXT NOT NULL,
  short_name   TEXT,
  icon_url     TEXT
);

CREATE TABLE IF NOT EXISTS locations (
  location_id  INT PRIMARY KEY,
  city         TEXT,
  stadium      TEXT
);

CREATE TABLE IF NOT EXISTS groups (                -- Spieltag / Vorrunde / Finale ...
  group_id     INT PRIMARY KEY,
  league_id    INT NOT NULL REFERENCES leagues ON DELETE CASCADE,
  group_order  INT NOT NULL,
  group_name   TEXT
);
CREATE INDEX IF NOT EXISTS ix_groups_league_order ON groups (league_id, group_order);

CREATE TABLE IF NOT EXISTS matches (
  match_id         INT PRIMARY KEY,
  league_id        INT NOT NULL REFERENCES leagues,
  group_id         INT REFERENCES groups,
  team1_id         INT REFERENCES teams,
  team2_id         INT REFERENCES teams,
  team1_group_name TEXT,                           -- teamGroupName của team1 (vd "Gruppe A" ở giải đấu loại)
  team2_group_name TEXT,                           -- teamGroupName của team2
  location_id      INT REFERENCES locations,
  match_time_utc   TIMESTAMPTZ,
  match_time_local TIMESTAMP,
  time_zone_id     TEXT,
  is_finished      BOOLEAN NOT NULL,
  viewers          INT,
  api_updated_at   TIMESTAMP                       -- lastUpdateDateTime (KHÔNG dùng để so sánh thay đổi)
);
CREATE INDEX IF NOT EXISTS ix_matches_league   ON matches (league_id);
CREATE INDEX IF NOT EXISTS ix_matches_group    ON matches (group_id);
CREATE INDEX IF NOT EXISTS ix_matches_time     ON matches (match_time_utc);

CREATE TABLE IF NOT EXISTS match_results (
  result_id        INT PRIMARY KEY,
  match_id         INT NOT NULL REFERENCES matches ON DELETE CASCADE,
  result_name      TEXT,
  result_order     INT,
  result_type_id   INT,
  result_type_kind TEXT,
  description      TEXT,
  points_team1     INT,
  points_team2     INT
);
CREATE INDEX IF NOT EXISTS ix_results_match ON match_results (match_id);

CREATE TABLE IF NOT EXISTS goals (
  goal_id        INT PRIMARY KEY,
  match_id       INT NOT NULL REFERENCES matches ON DELETE CASCADE,
  minute         INT,
  scorer_id      INT,
  scorer_name    TEXT,
  scoring_team_id INT,
  score_team1    INT,
  score_team2    INT,
  is_penalty     BOOLEAN,
  is_own_goal    BOOLEAN,
  is_overtime    BOOLEAN,
  comment        TEXT
);
CREATE INDEX IF NOT EXISTS ix_goals_match ON goals (match_id);

-- ---------------------------------------------------------------------------
-- Metadata của worker: KHÔNG đưa vào publication
-- group_order = -1  -> reconcile cả mùa ; -2 -> refresh danh sách league
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sync_state (
  league_shortcut TEXT NOT NULL,
  season          INT  NOT NULL,
  group_order     INT  NOT NULL,
  last_change_raw TEXT,                            -- chuỗi thô từ /getlastchangedate (so sánh bằng ==)
  last_checked_at TIMESTAMPTZ,
  last_synced_at  TIMESTAMPTZ,
  PRIMARY KEY (league_shortcut, season, group_order)
);

CREATE TABLE IF NOT EXISTS raw_api_log (             -- tuỳ chọn (OPENLIGA_RAW_LOG=1); cũng không publish
  id           BIGSERIAL PRIMARY KEY,
  endpoint     TEXT NOT NULL,
  fetched_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
  payload_sha  TEXT NOT NULL,
  payload      JSONB
);
CREATE INDEX IF NOT EXISTS ix_raw_log_endpoint ON raw_api_log (endpoint, id DESC);

-- ---------------------------------------------------------------------------
-- CDC: REPLICA IDENTITY FULL để event UPDATE/DELETE có đủ "before"
-- ---------------------------------------------------------------------------
ALTER TABLE matches       REPLICA IDENTITY FULL;
ALTER TABLE match_results REPLICA IDENTITY FULL;
ALTER TABLE goals         REPLICA IDENTITY FULL;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = 'football_pub') THEN
    CREATE PUBLICATION football_pub FOR TABLE
      sports, result_types, leagues, league_result_infos,
      teams, locations, groups, matches, match_results, goals;
  END IF;
END $$;
-- Lưu ý: KHÔNG tạo replication slot ở đây. Để Debezium tự tạo (tránh slot mồ côi giữ WAL).

-- ---------------------------------------------------------------------------
-- Views dẫn xuất (không mirror /getbltable; tính lại từ matches)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW v_match_scores AS
SELECT m.match_id, m.league_id, m.group_id, m.team1_id, m.team2_id, m.is_finished,
       max(r.points_team1) FILTER (WHERE r.result_type_kind = 'HalfTime')       AS ht1,
       max(r.points_team2) FILTER (WHERE r.result_type_kind = 'HalfTime')       AS ht2,
       max(r.points_team1) FILTER (WHERE r.result_type_kind = 'After90Minutes') AS ft1,
       max(r.points_team2) FILTER (WHERE r.result_type_kind = 'After90Minutes') AS ft2
FROM matches m
LEFT JOIN match_results r USING (match_id)
GROUP BY m.match_id;

CREATE OR REPLACE VIEW v_league_table AS
WITH s AS (
  SELECT league_id, team1_id AS team_id, ft1 AS gf, ft2 AS ga FROM v_match_scores
   WHERE is_finished AND ft1 IS NOT NULL AND ft2 IS NOT NULL AND team1_id IS NOT NULL
  UNION ALL
  SELECT league_id, team2_id, ft2, ft1 FROM v_match_scores
   WHERE is_finished AND ft1 IS NOT NULL AND ft2 IS NOT NULL AND team2_id IS NOT NULL
)
SELECT league_id, team_id,
       count(*)                                  AS matches,
       count(*) FILTER (WHERE gf > ga)           AS won,
       count(*) FILTER (WHERE gf = ga)           AS draw,
       count(*) FILTER (WHERE gf < ga)           AS lost,
       sum(gf)                                   AS goals,
       sum(ga)                                   AS opponent_goals,
       sum(gf - ga)                              AS goal_diff,
       sum(CASE WHEN gf > ga THEN 3 WHEN gf = ga THEN 1 ELSE 0 END) AS points
FROM s GROUP BY league_id, team_id;
