{{ config(materialized='external') }}
{# Bảng tham chiếu giải đấu: mỗi nguồn gọi giải bằng 1 mã khác nhau. Thêm giải mới = thêm 1 dòng. #}

select * from (values
    ('EPL',          'Premier League',        'England', 1, 'PL',  'E0',  'EPL',        39,  'soccer_epl',                'EPL',          NULL),
    ('CHAMPIONSHIP', 'EFL Championship',      'England', 2, 'ELC', 'E1',  NULL,         40,  NULL,                        'Championship', NULL),
    ('LALIGA',       'La Liga',               'Spain',   1, 'PD',  'SP1', 'La_Liga',    140, 'soccer_spain_la_liga',      'LaLiga',       NULL),
    ('BUNDESLIGA',   'Bundesliga',            'Germany', 1, 'BL1', 'D1',  'Bundesliga', 78,  'soccer_germany_bundesliga', 'Bundesliga',   'bl1'),
    ('SERIEA',       'Serie A',               'Italy',   1, 'SA',  'I1',  'Serie_A',    135, 'soccer_italy_serie_a',      NULL,           NULL),
    ('LIGUE1',       'Ligue 1',               'France',  1, 'FL1', 'F1',  'Ligue_1',    61,  'soccer_france_ligue_one',   NULL,           NULL),
    ('UCL',          'UEFA Champions League', 'Europe',  0, 'CL',  NULL,  NULL,         2,   NULL,                        NULL,           NULL),
    ('RFPL',         'Russian Premier League','Russia',  1, NULL,  NULL,  'RFPL',       235, NULL,                        NULL,           NULL)
) as t(competition_key, competition_name, country, tier, fdo_code, couk_division,
       understat_league, api_football_league_id, odds_sport_key, thesportsdb_slug, openliga_shortcut)

