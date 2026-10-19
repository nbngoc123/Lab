"""Read-only MLflow Registry -> PostgreSQL monitoring snapshot for Superset."""
import os
from datetime import datetime, timezone
import pandas as pd
from sqlalchemy import create_engine, text
from mlflow.tracking import MlflowClient

MODELS = {'topic':'football-text-topic','sentiment':'football-text-sentiment','anomaly':'football-text-anomaly'}

def main():
    tracking=os.environ['MLFLOW_TRACKING_URI']
    pg=os.environ['POSTGRES_SQLALCHEMY_URI']
    schema=os.getenv('DWH_SCHEMA','football_text_ai')
    if not schema.replace('_','').isalnum() or not schema[0].isalpha():
        raise ValueError('Invalid DWH_SCHEMA')
    c=MlflowClient(tracking_uri=tracking)
    now=datetime.now(timezone.utc)
    rows=[]
    for kind,name in MODELS.items():
        version=None;status='missing_alias';description=''
        try:
            version=c.get_model_version_by_alias(name,'champion')
            status='registered'
        except Exception as exc:
            status='unavailable';description=str(exc)[:500]
        if version is None:
            rows.append(dict(model_role=kind,registered_name=name,version=None,alias='champion',status=status,
                             source_run_id=None,model_uri=f'models:/{name}@champion',training_started_at=None,
                             training_metrics_json='{}',training_params_json='{}',error_message=description,checked_at=now))
            continue
        run=c.get_run(version.run_id) if version.run_id else None
        import json
        rows.append(dict(model_role=kind,registered_name=name,version=int(version.version),alias='champion',status=status,
                         source_run_id=version.run_id,model_uri=f'models:/{name}@champion',
                         training_started_at=pd.to_datetime(run.info.start_time,unit='ms',utc=True) if run else None,
                         training_metrics_json=json.dumps(run.data.metrics if run else {},ensure_ascii=False),
                         training_params_json=json.dumps(run.data.params if run else {},ensure_ascii=False),
                         error_message='',checked_at=now))
    df=pd.DataFrame(rows)
    eng=create_engine(pg,pool_pre_ping=True)
    try:
        with eng.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
            df.to_sql('text_mlflow_models',conn,schema=schema,if_exists='replace',index=False)
        print(df[['model_role','version','status','source_run_id']].to_string(index=False))
    finally:eng.dispose()

if __name__=='__main__':main()
