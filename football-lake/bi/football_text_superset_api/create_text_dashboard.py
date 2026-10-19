"""Provision ONE Football Text Intelligence dashboard through Superset REST API.
Never writes DWH, and never silently deletes charts or dashboards.
Compatible with the common Apache Superset v3-v6 REST contracts; chart controls can vary by version.
"""
import os,json,argparse,re
from collections import defaultdict
import requests
from dotenv import load_dotenv

load_dotenv()
BASE=os.getenv('SUPERSET_URL','http://localhost:8088').rstrip('/')
DB_ID=int(os.getenv('SUPERSET_DATABASE_ID','0'))
SCHEMA=os.getenv('DWH_SCHEMA','football_text_ai')
DASHBOARD_NAME='Football Text Intelligence 360 | ML & Data Observability'

# Chart specs: chart-name, physical table, viz_type, time_dimension, metrics, groupby, optional filters/cols.
# Prefer physical datasets so Superset native filters and permissions work predictably.
SPECS=[
# Section A — model and data health
('A1 · Models Registered','text_mlflow_models','big_number_total',None,[("COUNT(*) FILTER (WHERE status = 'registered')",'Models Registered')],[],None),
('A2 · Model Registry Status','text_mlflow_models','table',None,[],['model_role','registered_name','version','alias','status','source_run_id','training_started_at','training_metrics_json','training_params_json','checked_at'],None),
('A3 · Documents Processed','text_predictions','big_number_total',None,[('COUNT(*)','Documents')],[],None),
('A4 · Average Sentiment Confidence','text_predictions','big_number_total',None,[('AVG(sentiment_confidence)','Confidence')],[],None),
('A5 · Source Coverage','text_predictions','pie',None,[('COUNT(*)','Documents')],['source_type'],None),
('A6 · Pipeline / Model Lineage','text_predictions','table',None,[],['batch_id','reference_date','topic_model_uri','sentiment_model_uri','anomaly_model_uri'],None),
# Section B — opinion and content
('B1 · Daily Content Volume','text_sentiment_daily','echarts_timeseries_line','event_date',[('SUM(docs)','Documents')],['source_type'],None),
('B2 · Positive / Neutral / Negative','text_predictions','pie',None,[('COUNT(*)','Documents')],['sentiment_label'],None),
('B3 · Sentiment Balance Over Time','text_scope_daily','echarts_timeseries_line','event_date',[('AVG(sentiment_balance)','Balance')],['scope'],None),
('B4 · Sources by Negative Rate','text_sentiment_daily','echarts_timeseries_bar',None,[('SUM(negative)::float / NULLIF(SUM(docs),0)','Negative Rate')],['source_type'],None),
# Section C — topics and momentum
('C1 · Most Discussed Topics','text_topic_summary','echarts_timeseries_bar',None,[('SUM(docs)','Documents')],['topic_name'],None),
('C2 · Topic Discussion Over Time','text_topic_daily','echarts_timeseries_line','event_date',[('SUM(docs)','Documents')],['topic_name'],None),
('C3 · Topic Growth: Last 7 Days','text_topic_trends','table',None,[],['topic_name','source_type','scope','docs_recent','docs_previous','share_lift','share_delta_pp','trend_score'],None),
('C4 · Negative Topic Alerts','text_negative_alerts','table',None,[],['reference_date','topic_name','source_type','scope','docs_recent','negative_rate_recent','share_lift','trend_score'],None),
('C5 · Topic Anomaly Scores','text_topic_anomalies','table',None,[],['event_date','topic_name','source_type','scope','docs','anomaly_score','is_anomaly'],None),
# Section D — comparisons and entities
('D1 · Domestic vs International','text_scope_daily','echarts_timeseries_line','event_date',[('SUM(docs)','Documents')],['scope'],None),
('D2 · Entity Mentions','text_entity_mentions','table',None,[],['entity_name','entity_scope','source_type','event_date','sentiment_label','topic_id'],None),
('D3 · Entity Trends','text_entity_daily','echarts_timeseries_line','event_date',[('SUM(docs)','Mentions')],['entity_name'],None),
('D4 · Recent Football Content','text_predictions','table',None,[],['published_at','title','source_type','scope','topic_name','sentiment_label','sentiment_confidence','publisher'],None),
]
# There are optional entity tables only if the pipeline entity extraction produced them, but base tables should exist.

class Client:
    def __init__(self):
        self.s=requests.Session();self.s.headers['Accept']='application/json'
        token=os.getenv('SUPERSET_ACCESS_TOKEN')
        if token:
            self.s.headers['Authorization']='Bearer '+token
        else:
            user=os.getenv('SUPERSET_USERNAME');pw=os.getenv('SUPERSET_PASSWORD')
            if not user or not pw:raise RuntimeError('Set SUPERSET_ACCESS_TOKEN or SUPERSET_USERNAME/PASSWORD')
            out=self.s.post(BASE+'/api/v1/security/login',json={'username':user,'password':pw,'provider':'db','refresh':True},timeout=30)
            out.raise_for_status();self.s.headers['Authorization']='Bearer '+out.json()['access_token']
        try:
            csrf=self.req('GET','/api/v1/security/csrf_token/').get('result')
            if csrf:self.s.headers['X-CSRFToken']=csrf
        except Exception:pass
    def req(self,method,path,**kw):
        r=self.s.request(method,BASE+path,timeout=90,**kw)
        if r.status_code>=400:raise RuntimeError(f'{method} {path} returned HTTP {r.status_code}: {r.text[:1200]}')
        return r.json() if r.content else {}
    def list_all(self,path):
        out=[];page=0
        while True:
            q=json.dumps({'page':page,'page_size':100})
            response=self.req('GET',path,params={'q':q})
            result=response.get('result',[])
            if not isinstance(result,list):break
            out.extend(result)
            if len(result)<100:break
            page+=1
        return out

def unique_model_lines():
    return sorted(set(s[1] for s in SPECS))

def metric(expression,name):return {'expressionType':'SQL','sqlExpression':expression,'label':name,'optionName':'metric_'+re.sub(r'\W+','_',name.lower())}

def formdata(spec,dataset_id):
    name,table,viz,time_dim,metrics,groups,_=spec
    dat={'datasource':f'{dataset_id}__table','viz_type':viz,'row_limit':20,'adhoc_filters':[],
         'color_scheme':'supersetColors','show_legend':True,'show_values':False}
    if time_dim:
        dat.update({'granularity_sqla':time_dim,'time_grain_sqla':'P1D','x_axis':time_dim,'time_range':'No filter'})
    if viz=='big_number_total':dat.update({'metric':metric(*metrics[0]),'subheader':''})
    elif viz=='pie':dat.update({'metric':metric(*metrics[0]),'groupby':groups,'donut':True,'show_labels':True})
    elif viz in ('dist_bar', 'echarts_timeseries_bar'):
        cat_col = groups[0] if groups else (time_dim or 'category')
        dat.update({
            'viz_type': 'echarts_timeseries_bar',
            'x_axis': cat_col,
            'metrics': [metric(*m) for m in metrics],
            'groupby': [],
            'orientation': 'vertical',
            'show_legend': False,
            'order_desc': True,
            'row_limit': 20
        })
    elif viz=='echarts_timeseries_line':dat.update({'metrics':[metric(*m) for m in metrics],
                    'groupby':groups,'series':groups,'x_axis':time_dim,'time_grain_sqla':'P1D','show_legend':True})
    elif viz=='table':dat.update({'query_mode':'raw','all_columns':groups,'order_by_cols':[],
                                  'row_limit':100,'server_page_length':20,'table_timestamp_format':'smart_date'})
    return dat

def chart_payload(spec,dsid):
    fd=formdata(spec,dsid)
    return dict(slice_name=spec[0],viz_type=spec[2],datasource_id=dsid,datasource_type='table',params=json.dumps(fd),
                description='Created by football_text_superset_api. Edit via code rather than Superset UI.')

def ensure_dataset(c,table,existing,dry=False):
    lookup=[d for d in existing if d.get('table_name')==table and d.get('schema')==SCHEMA and
            (d.get('database',{}).get('id') if isinstance(d.get('database'),dict) else d.get('database'))==DB_ID]
    if lookup:return lookup[0]['id']
    if dry:return -1
    return c.req('POST','/api/v1/dataset/',json={'database':DB_ID,'schema':SCHEMA,'table_name':table})['id']

def layout(charts):
    """Superset dashboard position tree: titles as Markdown components and charts in 3-col rows."""
    root={'type':'ROOT','id':'ROOT_ID','children':['GRID_ID']}
    grid={'type':'GRID','id':'GRID_ID','parents':['ROOT_ID'],'children':[]}
    pos={'ROOT_ID':root,'GRID_ID':grid,'DASHBOARD_VERSION_KEY':'v2'}
    sections=[('01 · MODEL, DATA & PIPELINE HEALTH','A'),('02 · FOOTBALL SENTIMENT & DISCUSSION','B'),
              ('03 · TOPICS, TRENDS & ALERTS','C'),('04 · DOMESTIC / INTERNATIONAL & ENTITIES','D')]
    for sec_num,(title,prefix) in enumerate(sections):
        md_id=f'MARKDOWN-{sec_num}'
        head_id=f'ROW-H-{sec_num}'
        pos[md_id]={'type':'MARKDOWN','id':md_id,'parents':['ROOT_ID','GRID_ID',head_id],
                    'children':[],'meta':{'code':f'### {title}','height':40,'width':12}}
        pos[head_id]={'type':'ROW','id':head_id,'parents':['ROOT_ID','GRID_ID'],'children':[md_id],
                      'meta':{'background':'BACKGROUND_TRANSPARENT'}}
        grid['children'].append(head_id)
        items=[(s,cid) for s,cid in charts if s[0].startswith(prefix)]
        for row_num in range(0,len(items),3):
            row_id=f'ROW-{prefix}-{row_num//3}'
            chunk=items[row_num:row_num+3]
            pos[row_id]={'type':'ROW','id':row_id,'parents':['ROOT_ID','GRID_ID'],'children':[],
                         'meta':{'background':'BACKGROUND_TRANSPARENT'}}
            grid['children'].append(row_id)
            for spec,cid in chunk:
                node=f'CHART-{cid}'
                pos[node]={'type':'CHART','id':node,'parents':['ROOT_ID','GRID_ID',row_id],
                           'children':[],'meta':{'chartId':cid,'height':20 if spec[2]=='table' else 16,
                                                'width':12 if len(chunk)==1 else 6 if len(chunk)==2 else 4}}
                pos[row_id]['children'].append(node)
    return pos

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument('--dry-run',action='store_true',help='Only print specs; no API access, no changes')
    ap.add_argument('--skip-missing',action='store_true',help='Skip DWH tables not yet published')
    args=ap.parse_args()
    if args.dry_run:
        print(json.dumps({'dashboard':DASHBOARD_NAME,'tables':unique_model_lines(),
                          'charts':[s[0] for s in SPECS]},indent=2,ensure_ascii=False));return
    if not DB_ID:raise RuntimeError('SUPERSET_DATABASE_ID is required (Superset database connection ID)')
    c=Client()
    dslist=c.list_all('/api/v1/dataset/')
    datasets={}
    for t in unique_model_lines():
        try:datasets[t]=ensure_dataset(c,t,dslist)
        except Exception as e:
            if args.skip_missing:print('SKIP dataset',t,str(e)[:180]);continue
            raise
    existing_charts={x['slice_name']:x for x in c.list_all('/api/v1/chart/')}
    existing_dash={x['dashboard_title']:x for x in c.list_all('/api/v1/dashboard/')}
    dash=existing_dash.get(DASHBOARD_NAME)
    if dash:dash_id=dash['id']
    else:dash_id=c.req('POST','/api/v1/dashboard/',json={'dashboard_title':DASHBOARD_NAME,'published':True})['id']
    created=[]
    for spec in SPECS:
        if spec[1] not in datasets:continue
        p=chart_payload(spec,datasets[spec[1]])
        old=existing_charts.get(spec[0])
        if old:
            cid=old['id'];c.req('PUT',f'/api/v1/chart/{cid}',json=p)
        else:
            cid=c.req('POST','/api/v1/chart/',json={**p,'dashboards':[dash_id]})['id']
        created.append((spec,cid));print('Chart:',cid,spec[0])
    if not created:raise RuntimeError('No charts created. Publish DWH tables first.')
    payload={'dashboard_title':DASHBOARD_NAME,'published':True,'position_json':json.dumps(layout(created)),
             'json_metadata':json.dumps({'timed_refresh_immune_slices':[],'expanded_slices':{},'refresh_frequency':0,'color_scheme':'supersetColors'})}
    c.req('PUT',f'/api/v1/dashboard/{dash_id}',json=payload)
    print(f'\nREADY {BASE}/superset/dashboard/{dash_id}/ — {len(created)} charts')
    print('Check each chart with GET /api/v1/chart/{id}/data/; errors indicate version-specific viz params or missing data.')

if __name__=='__main__':main()
