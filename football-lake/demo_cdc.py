import pandas as pd
import time
from sqlalchemy import create_engine, text

def run_demo():
    print("Connecting to postgres-cdc...")
    engine = create_engine('postgresql://postgres:postgres@postgres-cdc:5432/testdb')
    
    print("Creating dummy data...")
    df = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'stadium_name': ['Old Trafford', 'Anfield', 'Emirates', 'Stamford Bridge', 'Etihad'],
        'capacity': [74000, 53000, 60000, 40000, 55000]
    })
    
    print("Writing to testdb...")
    df.to_sql('stadiums', engine, if_exists='replace', index=False)
    
    with engine.connect() as conn:
        conn.execute(text("ALTER TABLE stadiums REPLICA IDENTITY FULL;"))
        conn.commit()
    
    print("Data inserted and replica identity set!")
    
if __name__ == '__main__':
    run_demo()
