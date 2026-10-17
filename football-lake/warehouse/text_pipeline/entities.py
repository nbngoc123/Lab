"""Versionable rule-based entity baseline, matching research notebook aliases.
Replace aliases CSV with a reviewed project-specific catalog when ready.
"""
import re
from pathlib import Path
import pandas as pd

DEFAULT=[
('Manchester United','manchester united|man utd|man united|quỷ đỏ','international'),
('Manchester City','manchester city|man city','international'),
('Liverpool','liverpool','international'),('Arsenal','arsenal','international'),
('Chelsea','chelsea','international'),('Real Madrid','real madrid','international'),
('Barcelona','barcelona|barca|barça','international'),('Bayern Munich','bayern munich|bayern','international'),
('PSG','psg|paris saint-germain','international'),('Tottenham','tottenham|spurs','international'),
('Đội tuyển Việt Nam','đội tuyển việt nam|tuyển việt nam|vietnam national team|u23 việt nam','domestic_vn'),
('V-League','v-league|vleague|v.league','domestic_vn'),('Hà Nội FC','hà nội fc|hanoi fc','domestic_vn'),
('Hoàng Anh Gia Lai','hoàng anh gia lai|hagl','domestic_vn'),
('Nam Định','thép xanh nam định|nam định fc','domestic_vn'),
('Cristiano Ronaldo','cristiano ronaldo|ronaldo','international'),
('Lionel Messi','lionel messi|messi','international'),
('Champions League','champions league|uefa champions league|cúp c1','international')]

def compile_aliases(csv_path=None):
    if csv_path and Path(csv_path).exists():
        df=pd.read_csv(csv_path).fillna('')
        if not {'entity_name','aliases','scope'}.issubset(df.columns):
            raise ValueError('Alias CSV requires entity_name,aliases,scope')
    else:df=pd.DataFrame(DEFAULT,columns=['entity_name','aliases','scope'])
    result=[]
    for r in df.itertuples(index=False):
        for alias in str(r.aliases).split('|'):
            if len(alias.strip())>2:
                result.append((r.entity_name,r.scope,re.compile(r'(?<!\w)'+re.escape(alias.strip())+r'(?!\w)',re.IGNORECASE)))
    return result

def entity_tables(scored,alias_path=None):
    compiled=compile_aliases(alias_path)
    rows=[]
    for r in scored[['doc_id','event_date','source_type','title_clean','text_clean','topic_id','sentiment_label']].itertuples(index=False):
        text=(str(r.title_clean)+' '+str(r.text_clean)[:600]).lower()
        matches={name:scope for name,scope,regex in compiled if regex.search(text)}
        for name,scope in matches.items():
            rows.append({'doc_id':r.doc_id,'event_date':r.event_date,'source_type':r.source_type,
                         'entity_name':name,'entity_scope':scope,'topic_id':int(r.topic_id),'sentiment_label':r.sentiment_label})
    mentions=pd.DataFrame(rows,columns=['doc_id','event_date','source_type','entity_name','entity_scope','topic_id','sentiment_label'])
    if len(mentions):
        for label in ['positive','negative','neutral']:
            mentions[label]=mentions.sentiment_label.eq(label).astype(int)
        daily=mentions.groupby(['event_date','source_type','entity_name','entity_scope'],dropna=False).agg(
            docs=('doc_id','size'),positive=('positive','sum'),negative=('negative','sum'),neutral=('neutral','sum')).reset_index()
        for label in ['positive','negative','neutral']:
            daily[f'{label}_rate']=daily[label]/daily.docs
        daily['sentiment_balance']=daily.positive_rate-daily.negative_rate
    else:
        daily=pd.DataFrame(columns=['event_date','source_type','entity_name','entity_scope','docs','positive','negative','neutral','positive_rate','negative_rate','neutral_rate','sentiment_balance'])
    return mentions,daily
