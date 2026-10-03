import requests, json

# Kiểm tra wm26 - giải World Cup có group stage
r = requests.get("https://api.openligadb.de/getmatchdata/wm26/2026", timeout=30)
d = r.json() or []
print(f"Total matches: {len(d)}")
# In ra 3 trận đầu, xem teamGroupName
for m in d[:3]:
    t1 = m.get("team1") or {}
    t2 = m.get("team2") or {}
    print(f"Match {m['matchID']}: {t1.get('teamName')} (group={t1.get('teamGroupName')!r}) vs {t2.get('teamName')} (group={t2.get('teamGroupName')!r})")

# Đếm bao nhiêu trận có teamGroupName khác null
non_null = [(m["matchID"], (m.get("team1") or {}).get("teamGroupName")) for m in d if (m.get("team1") or {}).get("teamGroupName")]
print(f"\nMatches with teamGroupName: {len(non_null)}/{len(d)}")
if non_null:
    print("Sample:", non_null[:5])
    unique_vals = set(v for _, v in non_null)
    print("Unique group names:", unique_vals)
