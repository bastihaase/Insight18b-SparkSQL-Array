from flask import Flask
from flaskext.mysql import MySQL
import pandas as pd
from itertools import chain, cycle, izip


app = Flask(__name__)

# set up connection with MySQL database
mysql = MySQL()
app.config['MYSQL_DATABASE_USER'] = 'basti'
app.config['MYSQL_DATABASE_PASSWORD'] = '*********'
app.config['MYSQL_DATABASE_DB'] = 'processed'
app.config['MYSQL_DATABASE_HOST'] = '**********'
mysql.init_app(app)

# this makes sure our output will be printed correctly
@app.after_request
def treat_as_plain_text(response):
    response.headers["content-type"] = "text/plain"
    return response

# retrieve stats from mysql database and return results
@app.route("/")
def stats():
    conn = mysql.connect()
    cursor = conn.cursor()

    query = "SELECT  overlap, COUNT(*) occurences FROM metadata GROUP BY overlap"
    df = pd.read_sql(query, con=conn)

    output = "Amazon Purchase Data: How effective are recommendations? \n"
    output += "overlap = how many of the recommended items were also bought? \n"
    output += df.to_String(index=False)
    return output

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80)
