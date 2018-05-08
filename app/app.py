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
app.config['MYSQL_DATABASE_PASSWORD'] = 'metadata'
app.config['MYSQL_DATABASE_DB'] = 'processed'
app.config['MYSQL_DATABASE_HOST'] = 'ec2-35-160-106-92.us-west-2.compute.amazonaws.com'
mysql.init_app(app)




@app.route("/")
def batch_stats():
    # retrieve stats from mysql database and return results
    conn = mysql.connect()
    cursor = conn.cursor()

    query = "SELECT  overlap, COUNT(*) occurences FROM output GROUP BY overlap"
    data = pd.read_sql(query, con=conn).values.tolist()

    # rearrange data for bar plot
    legend = 'Bought and Shown'
    labels = [x[0] for x in data]
    values = [x[1] for x in data]
    #maximum = max(values)
    #values = [math.ceil(100 * x / float(maximum)) for x in values]

    streaming_query = "SELECT overlap, COUNT(*) occurences FROM streaming GROUP BY overlap"

    streaming_data = pd.read_sql(streaming_query, con=conn).values.tolist()

    # rearrange data for bar plot;
    #-1 and 0  in labels have to be added together
    streaming_legend = 'Bought and Shown'
    streaming_labels = [x[0] for x in streaming_data[1:]]
    streaming_values = [x[1] for x in streaming_data[1:]]
    streaming_values[0] += streaming_data[0][1]


    #return the bar plot
    return render_template('chart.html', values=values, labels=labels, legend=legend, svalues=streaming_values, slabels=streaming_labels, slegend=streaming_legend)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=80)
