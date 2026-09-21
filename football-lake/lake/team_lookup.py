"""
lake/team_lookup.py
-------------------
Helper module: resolve tên đội bóng từ bất kỳ nguồn nào → team_key chuẩn.
Dựa trên seed/team_alias.csv.

Cách dùng:
    from lake.team_lookup import resolve_team, team_patterns_from_alias, ALIAS_MAP

    team_key = resolve_team("Man City", source="fd")          # → "Manchester City"
    team_key = resolve_team("Manchester City F.C.", source="wikidata")  # → "Manchester City"
    # Nếu không tìm thấy → trả về None (không raise exception)
"""
from __future__ import annotations
import re
from pathlib import Path
import pandas as pd

# Tự động tìm seed/team_alias.csv dù gọi từ đâu trong project
_SEED_PATH = Path(__file__).parent.parent / "seed" / "team_alias.csv"


def _load_alias() -> pd.DataFrame:
    if not _SEED_PATH.exists():
        raise FileNotFoundError(
            f"Không tìm thấy {_SEED_PATH}. "
            "Hãy đảm bảo seed/team_alias.csv tồn tại trong root project."
        )
    df = pd.read_csv(_SEED_PATH, comment="#", skipinitialspace=True)
    df.columns = df.columns.str.strip()
    df = df.dropna(subset=["source", "alias", "team_key"])
    df["alias_lower"] = df["alias"].str.strip().str.lower()
    return df


# Lazy load + cache
_ALIAS_DF: pd.DataFrame | None = None


def _get_df() -> pd.DataFrame:
    global _ALIAS_DF
    if _ALIAS_DF is None:
        _ALIAS_DF = _load_alias()
    return _ALIAS_DF


# Expose toàn bộ map dạng dict để các module khác dùng
@property
def ALIAS_MAP() -> dict[tuple[str, str], str]:
    df = _get_df()
    return {(row.source, row.alias_lower): row.team_key for row in df.itertuples()}


def resolve_team(name: str, source: str | None = None) -> str | None:
    """
    Tìm team_key chuẩn cho một tên đội.

    Args:
        name:   Tên đội cần tra (VD "Man City", "Manchester City F.C.")
        source: Nguồn dữ liệu (VD "fd", "wikidata", "understat").
                Nếu None → tra tất cả source, lấy kết quả đầu tiên tìm được.

    Returns:
        team_key (str) hoặc None nếu không tìm thấy.
    """
    if not name or not isinstance(name, str):
        return None

    df = _get_df()
    name_lower = name.strip().lower()

    if source:
        mask = (df["source"] == source) & (df["alias_lower"] == name_lower)
        rows = df[mask]
        if not rows.empty:
            return rows.iloc[0]["team_key"]
        # Fallback: tìm không phân biệt source
        mask_all = df["alias_lower"] == name_lower
        rows = df[mask_all]
        return rows.iloc[0]["team_key"] if not rows.empty else None
    else:
        rows = df[df["alias_lower"] == name_lower]
        return rows.iloc[0]["team_key"] if not rows.empty else None


def add_team_key(df: pd.DataFrame, col: str, source: str,
                 out_col: str = "team_key") -> pd.DataFrame:
    """
    Thêm cột team_key vào DataFrame.

    Args:
        df:      DataFrame cần thêm cột
        col:     Tên cột chứa tên đội cần tra
        source:  Nguồn dữ liệu (dùng để ưu tiên tra theo source)
        out_col: Tên cột kết quả (mặc định "team_key")
    """
    df = df.copy()
    df[out_col] = df[col].apply(lambda x: resolve_team(x, source=source))
    n_missing = df[out_col].isna().sum()
    if n_missing > 0:
        missing_teams = df[df[out_col].isna()][col].unique().tolist()
        print(f"  ⚠ team_lookup: {n_missing} dòng chưa map được "
              f"(source={source}): {missing_teams[:10]}")
    return df


def team_patterns_from_alias(source: str | None = None) -> dict[str, str]:
    """
    Sinh dict {team_key: regex_pattern} từ alias CSV, dùng cho entity linking.
    Chỉ lấy các alias từ source chỉ định (VD "reddit") hoặc tất cả nếu None.
    Các alias ngắn hơn 4 ký tự bị loại (quá mơ hồ).

    Returns:
        {team_key: r"\\b(alias1|alias2|...)\\b"}
    """
    df = _get_df()
    if source:
        df = df[df["source"] == source]

    patterns: dict[str, list[str]] = {}
    for row in df.itertuples():
        alias = row.alias.strip()
        # Bỏ alias quá ngắn hoặc mơ hồ
        if len(alias) < 4:
            continue
        # Escape regex special chars
        escaped = re.escape(alias.lower())
        patterns.setdefault(row.team_key, []).append(escaped)

    return {
        team: r"\b(" + "|".join(sorted(set(aliases), key=len, reverse=True)) + r")\b"
        for team, aliases in patterns.items()
        if aliases
    }


if __name__ == "__main__":
    print("=== Test team_lookup ===")
    tests = [
        ("Man City", "fd"),
        ("Manchester City F.C.", "wikidata"),
        ("Arsenal FC", "apifootball"),
        ("Wolves", "fd"),
        ("Tottenham", "understat"),
        ("Unknown Club", "fd"),
    ]
    for name, src in tests:
        result = resolve_team(name, src)
        print(f"  {src:15} | {name:30} → {result}")

    print("\n=== Reddit patterns ===")
    pats = team_patterns_from_alias(source="reddit")
    for team, pat in list(pats.items())[:5]:
        print(f"  {team}: {pat}")
