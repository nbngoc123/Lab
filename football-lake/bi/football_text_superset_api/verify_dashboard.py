"""Verify Superset dashboard/charts entirely through REST API (no UI)."""
import argparse,json
from create_text_dashboard import Client

def main():
    a=argparse.ArgumentParser();a.add_argument('dashboard_id',type=int);a.add_argument('--query-data',action='store_true');args=a.parse_args()
    c=Client()
    dash=c.req('GET',f'/api/v1/dashboard/{args.dashboard_id}')
    result=dash.get('result',{})
    print('Dashboard:',result.get('dashboard_title'),'ID:',args.dashboard_id)
    charts=c.req('GET',f'/api/v1/dashboard/{args.dashboard_id}/charts').get('result',[])
    if isinstance(charts,dict):charts=charts.get('charts',[])
    failed=0
    for chart in charts:
        cid=chart.get('id') or chart.get('slice_id')
        name=chart.get('slice_name','unnamed')
        if not cid:continue
        state='OK metadata'
        try:
            detail=c.req('GET',f'/api/v1/chart/{cid}')
            if args.query_data:
                data=c.req('GET',f'/api/v1/chart/{cid}/data/')
                state='OK data'
                if data.get('errors'):
                    raise RuntimeError(str(data['errors'])[:400])
        except Exception as e:
            failed+=1;state='FAILED '+str(e)[:300]
        print(f'{cid:>5} {name}: {state}')
    print(f'Charts: {len(charts)}; failed: {failed}')
    if failed:raise SystemExit(1)
if __name__=='__main__':main()
