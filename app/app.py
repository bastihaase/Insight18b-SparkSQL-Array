from flask import Flask
from flaskext.mysql import MySQL
from flask import render_template
import math
import pandas as pd
from itertools import chain, cycle, izip


app = Flask(__name__)

# set up connection with MySQL database
mysql = MySQL()
app.config['MYSQL_DATABASE_USER'] = 'basti'
app.config['MYSQL_DATABASE_PASSWORD'] = '**********'
app.config['MYSQL_DATABASE_DB'] = 'processed'
app.config['MYSQL_DATABASE_HOST'] = '**********'
mysql.init_app(app)




@app.route("/")
def stats():
    # retrieve stats from mysql database and return results
    conn = mysql.connect()
    cursor = conn.cursor()

    query = "SELECT  overlap, COUNT(*) occurences FROM output GROUP BY overlap"
    data = pd.read_sql(query, con=conn).values.tolist()

    # rearrange data for bar plot
    legend = 'Bought and Shown'
    labels = [x[0] for x in data]
    values = [x[1] for x in data]
    maximum = max(values)
    values = [math.ceil(100 * x / float(maximum)) for x in values]


    #return the bar plot
    return render_template('chart.html', values=values, labels=labels, legend=legend)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80)
