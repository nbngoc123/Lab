"""Keep identical features to the training notebook, including intentionally conservative language guesses."""
import html
import re
import numpy as np
import pandas as pd

VN_TERMS=['việt nam','vietnam','v-league','vleague','v.league','hà nội fc','hanoi fc','đội tuyển việt nam','bóng đá việt nam','u23 việt nam','hoàng anh gia lai','thể công viettel','nam định fc','hải phòng fc']
INTL_TERMS=['premier league','champions league','la liga','bundesliga','serie a','ligue 1','arsenal','chelsea','liverpool','barcelona','real madrid','manchester united','man city','bayern','psg','inter milan','juventus','tottenham','cristiano ronaldo','messi']

def col(df,key,default=''):
    return df[key] if key in df.columns else pd.Series(default,index=df.index)

def clean(value):
    if pd.isna(value):return ''
    text=html.unescape(str(value));text=re.sub(r'https?://\S+',' ',text)
    return re.sub(r'\s+',' ',text).strip()

def lang(text,hint):
    if isinstance(hint,str) and hint.lower() in ('vi','en'):return hint.lower()
    low=text.lower()
    if re.search(r'[ăâđêôơưĂÂĐÊÔƠƯ]',text) or len(re.findall(r'\b(?:đội|trận|bóng|đá|cầu|thủ|huấn|luyện|chiến|thắng|tuyển|vô|địch|chuyển|nhượng)\b',low))>=2:return 'vi'
    return 'und'

def scope(text):
    low=text.lower()
    if any(w in low for w in VN_TERMS):return 'domestic_vn'
    if any(w in low for w in INTL_TERMS):return 'international'
    return 'unknown'

def normalize_one(key,df):
    if key.endswith('google_news'):
        src='google_news'; ident=col(df,'news_id'); title=col(df,'title');body=col(df,'summary');text=col(df,'text');publisher=col(df,'publisher');url=col(df,'url');video=col(df,'video_id');engagement=col(df,'engagement',0)
    elif key.endswith('football_news'):
        src='football_news';ident=col(df,'news_id');title=col(df,'title');body=col(df,'text');text=col(df,'text');publisher=col(df,'source_name');url=col(df,'url');video=col(df,'video_id');engagement=col(df,'engagement',0)
    elif key.endswith('youtube_videos'):
        src='youtube_video';ident=col(df,'video_id');title=col(df,'title');body=col(df,'description');text=col(df,'text');publisher=col(df,'channel_title');url='https://www.youtube.com/watch?v='+ident.astype(str);video=col(df,'video_id');engagement=col(df,'n_comments_collected',0)
    elif key.endswith('youtube_comments'):
        src='youtube_comment';ident=col(df,'comment_id');title=col(df,'video_title');body=col(df,'text');text=col(df,'text');publisher=col(df,'video_channel_title');video=col(df,'video_id');url='https://www.youtube.com/watch?v='+video.astype(str);engagement=col(df,'like_count',0)
    else:raise ValueError(key)
    d=pd.DataFrame({'doc_id':src+':'+ident.fillna('').astype(str),'source_type':src,'title':title.fillna('').astype(str),
                    'body':body.fillna('').astype(str),'raw_text':text.fillna('').astype(str),
                    'publisher':publisher.fillna('').astype(str),'url':pd.Series(url,index=df.index).fillna('').astype(str),
                    'video_id':video.fillna('').astype(str),'lang_hint':col(df,'language').fillna('').astype(str),
                    'published_at':pd.to_datetime(col(df,'published_at'),utc=True,errors='coerce'),
                    'engagement':pd.to_numeric(engagement,errors='coerce').fillna(0).clip(lower=0)})
    d['text_clean']=d.raw_text.map(clean);d['title_clean']=d.title.map(clean);d['body_clean']=d.body.map(clean)
    if src=='youtube_comment':
        d['topic_input']=(d.title_clean+' '+d.text_clean.str.slice(0,180)).str.slice(0,800)
        d['sentiment_input']=d.text_clean.str.slice(0,500)
    else:
        d['topic_input']=(d.title_clean+' '+d.body_clean.str.slice(0,700)).str.slice(0,1000)
        d['sentiment_input']=(d.title_clean+' '+d.body_clean.str.slice(0,200)).str.slice(0,500)
    d['language']=[lang(t,h) for t,h in zip(d.text_clean,d.lang_hint)]
    d['scope']=[scope(t) for t in d.topic_input]
    return d

def normalize_all(raw):
    d=pd.concat([normalize_one(k,v) for k,v in raw.items()],ignore_index=True)
    d=d[d.published_at.notna() & d.text_clean.str.len().gt(0) & ~d.doc_id.str.endswith(':')]
    d=d.drop_duplicates('doc_id',keep='last').reset_index(drop=True)
    d['event_date']=d.published_at.dt.tz_convert('UTC').dt.tz_localize(None).dt.normalize()
    return d
