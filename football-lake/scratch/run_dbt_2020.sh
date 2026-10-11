#!/bin/bash
source ~/venv/bin/activate
cd /home/azureuser/lab/Lab/football-lake/dbt
dbt build --profiles-dir . --vars '{"openliga_min_season": 2020}'
