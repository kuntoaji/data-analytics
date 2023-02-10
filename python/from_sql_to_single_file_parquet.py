# Python migrate data from sql to parquet
# see details on railsmine.net

import pandas as pd
import sqlalchemy # use version 1.x, pip install 'SQLAlchemy<2.0.0'
from fastparquet import write

# connect to database
engine = sqlalchemy.create_engine('postgresql+psycopg2://exampleuser:examplepassword@localhost/example_db')

# batch processing
batch_size = 100
total_rows = pd.read_sql("SELECT count(*) FROM example_table", engine).iloc[0, 0]
num_batches = total_rows // batch_size + (total_rows % batch_size > 0)

for i in range(num_batches):
    start = i * batch_size
    end = (i + 1) * batch_size
    df = pd.read_sql("SELECT id, email FROM example_table LIMIT {} OFFSET {}".format(batch_size, start), engine)

    # check if file exists. if not, create it. if so, append to it.
    if i == 0:
        write('output.parquet', df)
    else:
        write('output.parquet', df, append=True)
