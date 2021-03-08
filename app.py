from flask import Flask, render_template, request, redirect, url_for
from cassandra.cluster import Cluster
from datetime import datetime

app = Flask(__name__)

# Connect cassandra db
cluster = Cluster()
session = cluster.connect("hepsiburada")

my_dict = {}
my_list = []
@app.route("/index", methods=["GET", "POST"])
def index():
    if request.method == 'POST':
        data = request.form
        name = data["name"]

        query = "select userid,productid,timestamp from user where userid='{}' allow filtering".format(name)
        user_data = session.execute(query)
        for row in user_data:
            my_dict["{}".format(row[1])] = (row[2])
        list_value = list(my_dict.values())
        #print(list_value)
        for i in list_value:
            removed = i.split("+")[0]
            my_list.append(removed)
        date = [str(datetime.strptime(holiday, '%Y-%d-%m %H:%M:%S.%f')) for holiday in my_list]
        print(date)
        my_dict2 = {y:x for x,y in my_dict.items()}
        #print(sorted(date,reverse=True))
        #print(my_dict2)
        values = list(my_dict2.values())
        my_dict3 = dict(zip(date, values))
        desc_result = sorted(my_dict3,reverse=True)
        result1 = desc_result[0]
        result2 = desc_result[1]
        result3 = desc_result[2]
        result4 = desc_result[3]
        result5 = desc_result[4]
        # passes user_data variable into the index.html file.
        return render_template("index.html", my_dict3=my_dict3,result1=result1,result2=result2,result3=result3,result4=result4,result5=result5,name=name)

    return render_template("index.html")


if __name__ == "__main__":
    app.run(debug=True)
