from flask import Flask,request,jsonify
import csv

def dict_to_csv(data, file_name):
    with open(file_name, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(data.keys())
        csvwriter.writerow(data.values())

app=Flask(__name__)
@app.route("/orderdetails",methods=["POST"])
def order_details():
    try:
        data=request.get_json(force=True)
        csv_file_path="output/order_data.csv"
        dict_to_csv(data,csv_file_path)
        response={
            "data":[],
            "msg":"execute succesfully",
            "status":200
        }

        return jsonify(response),200
    except Exception as e:
            print(e)
            response={
                "data":[],
                "msg":"error",
                "status":400
                }
            return jsonify(response),400
        

if __name__ == '__main__':
    app.run(debug=True)