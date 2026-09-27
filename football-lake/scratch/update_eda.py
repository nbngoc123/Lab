import json

path = 'd:/O/DOC/Năm 4/Data Mining/football-lake/notebooks/ipynb/feat/football_gold_features_eda_v5.ipynb'
with open(path, 'r', encoding='utf-8') as f:
    nb = json.load(f)

new_cells = [
    {
      'cell_type': 'markdown',
      'metadata': {},
      'source': [
        '## 17h. EDA nhóm H2H: `feature_team_h2h`\n',
        '\n',
        'Nguồn: Bảng tính toán từ lịch sử đối đầu trực tiếp giữa 2 đội (5 trận gần nhất).\n',
        'Chứa các thông tin quan trọng như tổng điểm giành được (pts_h2h), bàn thắng, thẻ phạt trong các lần chạm trán trước.'
      ]
    },
    {
      'cell_type': 'code',
      'execution_count': None,
      'metadata': {},
      'outputs': [],
      'source': [
        'h2h = catalog_info.get("feature_team_h2h", pd.DataFrame())\n',
        'print("feature_team_h2h:", len(h2h), "dòng")\n',
        'if not h2h.empty:\n',
        '    display(h2h.describe())\n'
      ]
    },
    {
      'cell_type': 'markdown',
      'metadata': {},
      'source': [
        '## 17i. EDA nhóm Tactical: `feature_team_tactical`\n',
        '\n',
        'Nguồn: **p24 (API-Football - Lineups)**.\n',
        'Cung cấp sơ đồ chiến thuật (formation) và số lượng cầu thủ được sử dụng.'
      ]
    },
    {
      'cell_type': 'code',
      'execution_count': None,
      'metadata': {},
      'outputs': [],
      'source': [
        'tac = catalog_info.get("feature_team_tactical", pd.DataFrame())\n',
        'print("feature_team_tactical:", len(tac), "dòng")\n',
        'if not tac.empty:\n',
        '    display(tac.describe())\n'
      ]
    },
    {
      'cell_type': 'markdown',
      'metadata': {},
      'source': [
        '## 17j. EDA nhóm YouTube: `feature_team_youtube`\n',
        '\n',
        'Nguồn: Youtube API.\n',
        'Phân tích sentiment (cảm xúc bình luận) và tương tác của video về đội bóng trước trận.'
      ]
    },
    {
      'cell_type': 'code',
      'execution_count': None,
      'metadata': {},
      'outputs': [],
      'source': [
        'yt = catalog_info.get("feature_team_youtube", pd.DataFrame())\n',
        'print("feature_team_youtube:", len(yt), "dòng")\n',
        'if not yt.empty:\n',
        '    display(yt.describe())\n'
      ]
    },
    {
      'cell_type': 'markdown',
      'metadata': {},
      'source': [
        '## 17k. EDA nhóm Media Spikes: `feature_media_spikes`\n',
        '\n',
        'Nguồn: Lượt xem Wikipedia (đột biến).\n',
        'Đo lường sự bất thường trong truyền thông (sa thải HLV, scandal, v.v.).'
      ]
    },
    {
      'cell_type': 'code',
      'execution_count': None,
      'metadata': {},
      'outputs': [],
      'source': [
        'spikes = catalog_info.get("feature_media_spikes", pd.DataFrame())\n',
        'print("feature_media_spikes:", len(spikes), "dòng")\n',
        'if not spikes.empty:\n',
        '    display(spikes.describe())\n'
      ]
    },
    {
      'cell_type': 'markdown',
      'metadata': {},
      'source': [
        '## 17l. EDA nhóm Trọng tài: `feature_referee`\n',
        '\n',
        'Nguồn: Thống kê nghiêm khắc của trọng tài bắt trận.\n',
        'Bao gồm trung bình thẻ vàng, thẻ đỏ, số pha phạm lỗi lịch sử của trọng tài này.'
      ]
    },
    {
      'cell_type': 'code',
      'execution_count': None,
      'metadata': {},
      'outputs': [],
      'source': [
        'ref = catalog_info.get("feature_referee", pd.DataFrame())\n',
        'print("feature_referee:", len(ref), "dòng")\n',
        'if not ref.empty:\n',
        '    display(ref.describe())\n'
      ]
    },
    {
      'cell_type': 'markdown',
      'metadata': {},
      'source': [
        '## 17m. EDA nhóm Di chuyển: `feature_distance`\n',
        '\n',
        'Nguồn: Tính toán khoảng cách địa lý giữa 2 sân vận động.\n',
        'Khám phá tác động của sự mệt mỏi khi đội khách phải di chuyển quãng đường xa.'
      ]
    },
    {
      'cell_type': 'code',
      'execution_count': None,
      'metadata': {},
      'outputs': [],
      'source': [
        'dist = catalog_info.get("feature_distance", pd.DataFrame())\n',
        'print("feature_distance:", len(dist), "dòng")\n',
        'if not dist.empty:\n',
        '    display(dist.describe())\n'
      ]
    }
]

# Find where to insert (before 17g)
idx = -1
for i, cell in enumerate(nb['cells']):
    if cell['cell_type'] == 'markdown' and '17g.' in ''.join(cell['source']):
        idx = i
        break

if idx != -1:
    nb['cells'][idx:idx] = new_cells
    
    # Also update 17c markdown
    for cell in nb['cells']:
        if cell['cell_type'] == 'markdown':
            src = ''.join(cell['source'])
            if '17c.' in src and 'The Odds API' in src:
                cell['source'] = [
                    '## 17c. EDA nhóm Market AH: `feature_market_ah` (Kèo Handicap & Tài/Xỉu)\n',
                    '\n',
                    'Nguồn: **p03 (football-data.co.uk)** — Cào trực tiếp tỷ lệ kèo Châu Á (AH) và Tài/Xỉu từ CSV. Do sử dụng dữ liệu lịch sử chuẩn xác, độ phủ (coverage) nay đã đạt xấp xỉ 100%.\n'
                ]
                break

    with open(path, 'w', encoding='utf-8') as f:
        json.dump(nb, f, indent=1, ensure_ascii=False)
    print('Notebook updated successfully!')
else:
    print('Could not find 17g.')
