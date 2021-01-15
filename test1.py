from job_context import JobContext
import utils.io.csv as iocsv

# This is a very simple test job to show the base functionality of the
# Context Manager and some of the initial io.csv funtions...


# Will be fed from Airflow
app = "MyTestApp"
environment = "dev"
snapshot = "2020-09-21"

with JobContext(app, environment, snapshot) as jc:
    path = "/home/stolt/Downloads/oscar_age_female.csv"
    df = iocsv.df_read_csv(jc, path, header=True)
    df.printSchema()
    df.show()
    su.iocsv_write_csv(jc, df, 'oscars/females.csv')
    

