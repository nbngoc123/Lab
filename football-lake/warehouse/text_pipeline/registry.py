"""Manual promotion from a verified MLflow artifact run to registered-model version.
Only register approved sklearn pipelines from the notebook. No automatic champion promotion.
"""
import argparse
import mlflow
from mlflow.tracking import MlflowClient
from dotenv import load_dotenv
import os

def main():
    load_dotenv()
    p=argparse.ArgumentParser()
    p.add_argument('--run-id',required=True)
    p.add_argument('--artifact-path',required=True,choices=['selected_topic_model','sentiment_student_model','anomaly_candidate','topic_model'])
    p.add_argument('--name',required=True)
    p.add_argument('--alias',default='',help='Optional, only after explicit manual approval, e.g. champion')
    a=p.parse_args()
    mlflow.set_tracking_uri(os.environ['MLFLOW_TRACKING_URI'])
    uri=f'runs:/{a.run_id}/{a.artifact_path}'
    mv=mlflow.register_model(uri,a.name)
    print('REGISTERED',a.name,'version',mv.version,'from',uri)
    if a.alias:
        MlflowClient().set_registered_model_alias(a.name,a.alias,mv.version)
        print('ALIAS',a.alias,'->',mv.version)
if __name__=='__main__':main()
