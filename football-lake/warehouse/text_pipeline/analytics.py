import numpy as np
import pandas as pd


def _daily(grouped):
    if grouped.empty:return grouped
    grouped['positive_rate']=grouped.positive/grouped.docs
    grouped['negative_rate']=grouped.negative/grouped.docs
    grouped['neutral_rate']=grouped.neutral/grouped.docs
    grouped['sentiment_balance']=grouped.positive_rate-grouped.negative_rate
    return grouped

def aggregate(docs,cfg,anomaly=None):
    reference=pd.Timestamp(cfg.reference_date).normalize() if cfg.reference_date else docs.event_date.max()
    if pd.isna(reference):raise ValueError('No valid published dates')
    left=reference-pd.Timedelta(days=cfg.lookback_days-1)
    d=docs.loc[docs.event_date.between(left,reference)].copy()
    if d.empty:raise ValueError('No records within lookback window')
    d['positive']=d.sentiment_label.eq('positive').astype(int)
    d['neutral']=d.sentiment_label.eq('neutral').astype(int)
    d['negative']=d.sentiment_label.eq('negative').astype(int)
    def metrics(keys):
        g=d.groupby(keys,dropna=False).agg(docs=('doc_id','size'),positive=('positive','sum'),neutral=('neutral','sum'),negative=('negative','sum'),engagement_sum=('engagement','sum')).reset_index()
        return _daily(g)
    daily=metrics(['event_date','source_type','scope'])
    topic_daily=metrics(['event_date','source_type','scope','topic_id','topic_name'])
    scope_daily=metrics(['event_date','scope'])
    counts=metrics(['source_type','scope','topic_id','topic_name'])
    dates_recent=set(pd.date_range(reference-pd.Timedelta(days=cfg.trend_days-1),reference,freq='D'))
    dates_previous=set(pd.date_range(reference-pd.Timedelta(days=2*cfg.trend_days-1),reference-pd.Timedelta(days=cfg.trend_days),freq='D'))
    d['window']=np.select([d.event_date.isin(dates_recent),d.event_date.isin(dates_previous)],['recent','previous'],default='other')
    windowed=d[d.window.ne('other')]
    keys=['source_type','scope','topic_id','topic_name']
    vol=windowed.groupby(keys+['window'],dropna=False).agg(docs=('doc_id','size'),negative=('negative','sum'),positive=('positive','sum')).reset_index()
    if not len(vol):
        trends=pd.DataFrame(columns=keys+['docs_recent','docs_previous','share_recent','share_previous','share_delta_pp','share_lift','negative_rate_recent','growing','trend_score'])
    else:
        trends=vol.pivot_table(index=keys,columns='window',values=['docs','negative','positive'],aggfunc='sum',fill_value=0).reset_index()
        trends.columns=['_'.join(x).rstrip('_') if isinstance(x,tuple) else x for x in trends.columns]
        for measure in ['docs','negative','positive']:
            for w in ['recent','previous']:
                if f'{measure}_{w}' not in trends:trends[f'{measure}_{w}']=0
        denominators=windowed.groupby(['source_type','scope','window']).size().unstack(fill_value=0).reset_index()
        for col in ['recent','previous']:
            if col not in denominators:denominators[col]=0
        denominators=denominators.rename(columns={'recent':'source_recent','previous':'source_previous'})
        trends=trends.merge(denominators,on=['source_type','scope'],how='left',validate='many_to_one')
        trends['share_recent']=trends.docs_recent/trends.source_recent.replace(0,np.nan)
        trends['share_previous']=trends.docs_previous/trends.source_previous.replace(0,np.nan)
        trends['share_delta_pp']=100*(trends.share_recent.fillna(0)-trends.share_previous.fillna(0))
        trends['share_lift']=(trends.share_recent.fillna(0)+.001)/(trends.share_previous.fillna(0)+.001)
        trends['negative_rate_recent']=trends.negative_recent/trends.docs_recent.replace(0,np.nan)
        trends['trend_score']=trends.share_delta_pp*np.sqrt(trends.docs_recent)
        trends['growing']=(trends.docs_recent>=cfg.min_trend)&(trends.share_lift>=cfg.share_lift)
    alerts=trends[(trends.growing)&(trends.negative_rate_recent>=cfg.negative_rate)].copy()
    alerts['alert_type']='negative_topic_burst'
    alerts['reference_date']=reference
    # Features exactly match notebook order and log1p preprocessing.
    features=['volume_log1p','negative_rate','positive_rate','sentiment_balance']
    anomaly_daily=topic_daily.copy()
    anomaly_daily['volume_log1p']=np.log1p(anomaly_daily.docs)
    if anomaly is not None and len(anomaly_daily):
        X=anomaly_daily[features].fillna(0)
        anomaly_daily['anomaly_score']=-anomaly.decision_function(X)
        anomaly_daily['is_anomaly']=anomaly.predict(X)==-1
    else:
        anomaly_daily['anomaly_score']=np.nan
        anomaly_daily['is_anomaly']=False
    anomaly_daily=anomaly_daily.drop(columns=['volume_log1p'])
    records=docs[['doc_id','source_type','published_at','event_date','title_clean','text_clean','publisher','url','video_id','language','scope','engagement','topic_id','topic_name','sentiment_label','sentiment_confidence','sentiment_method']].rename(columns={'title_clean':'title','text_clean':'text'}).copy()
    if records.doc_id.duplicated().any():raise AssertionError('duplicate doc_id after normalization')
    return {'text_predictions':records,'text_sentiment_daily':daily,'text_topic_daily':topic_daily,'text_scope_daily':scope_daily,
            'text_topic_summary':counts,'text_topic_trends':trends,'text_negative_alerts':alerts,'text_topic_anomalies':anomaly_daily},reference
