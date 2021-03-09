from flask import Flask, render_template, request, redirect, url_for
from cassandra.cluster import Cluster
from datetime import datetime

app = Flask(__name__)

# Connect cassandra db
cluster = Cluster()
# Cluter connect is used to connect database when host is localhost with default port
session = cluster.connect("hepsiburada")

# Create empty dictionary and list
my_dict = {}
my_list = []

# Create index route with method get and post
@app.route("/index", methods=["GET", "POST"])
def index():
    #Receive given user input when submitted
    if request.method == 'POST':
        data = request.form
        name = data["name"]
        #Query database to find all info about given user
        query = "select userid,productid,timestamp from user where userid='{}' allow filtering".format(name)
        user_data = session.execute(query)
        #Create dictionary from result set (Dictionary key productid and value timestamp)
        for row in user_data:
            my_dict["{}".format(row[1])] = (row[2])
        #Convert timestamp value to list
        list_value = list(my_dict.values())
        #print(list_value)
        #Remove unncessary part of the timestamp (+000)
        for i in list_value:
            removed = i.split("+")[0]
            my_list.append(removed)
        #Convert string to timestamp
        date = [str(datetime.strptime(holiday, '%Y-%d-%m %H:%M:%S.%f')) for holiday in my_list]
        print(date)
        #Change keys and values to sort easly according to timestamp value
        my_dict2 = {y:x for x,y in my_dict.items()}
        #print(sorted(date,reverse=True))
        #print(my_dict2)
        values = list(my_dict2.values())
        #Create new dictionary with timestamp schema
        my_dict3 = dict(zip(date, values))
        #Sort according to timestamp value
        desc_result = sorted(my_dict3,reverse=True)
        #Get last five product view
        result1 = desc_result[0]
        result2 = desc_result[1]
        result3 = desc_result[2]
        result4 = desc_result[3]
        result5 = desc_result[4]

        return render_template("index.html", my_dict3=my_dict3,result1=result1,result2=result2,result3=result3,result4=result4,result5=result5,name=name)

    return render_template("index.html")


if __name__ == "__main__":
    app.run(debug=True)
