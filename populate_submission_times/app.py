from chalice import Chalice
import json
import psycopg2 as pg
import os

app = Chalice(app_name='populate_submission_times')

def lambda_handler(event, context):
    
  def update_submission_times_from_wide_table(): 
    with pg.connect(os.environ["WAREHOUSE_DB_URI"]) as con:
      with con.cursor() as cur:
        cur.execute("""
              INSERT INTO submission_times(student_id, created_at)
              SELECT student_id, created_at
              FROM pisa_wide_table
              WHERE NOT EXISTS (
                SELECT student_id
                FROM submission_times
                WHERE submission_times.student_id=pisa_wide_table.student_id
                );""")
        con.commit()
  
  update_submission_times_from_wide_table()
   
  return {
    'statusCode': 200,
    'body': json.dumps('SUCCESS')
  }
