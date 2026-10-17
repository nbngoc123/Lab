from types import SimpleNamespace
import pandas as pd
from football_text_pipeline.analytics import aggregate

def test_trend_alert_with_two_windows():
    rows=[]
    ref=pd.Timestamp('2026-10-17')
    for i in range(14):
        date=ref-pd.Timedelta(days=i)
        for j in range(12):
            topic=0 if (i<7 and j<9) or (i>=7 and j<2) else 1
            rows.append({'doc_id':f'{i}_{j}','event_date':date,'published_at':date,'source_type':'google_news',
                'scope':'international','topic_id':topic,'topic_name':f'Topic {topic}',
                'sentiment_label':'negative' if topic==0 else 'positive',
                'sentiment_confidence':.8,'sentiment_method':'test','engagement':0,
                'title_clean':'a','text_clean':'b','publisher':'p','url':'','video_id':'','language':'en'})
    cfg=SimpleNamespace(reference_date='2026-10-17',lookback_days=60,trend_days=7,
       min_trend=5,share_lift=1.25,negative_rate=.4)
    tables,_=aggregate(pd.DataFrame(rows),cfg)
    assert len(tables['text_predictions'])==168
    assert 0 in tables['text_negative_alerts'].topic_id.tolist()
