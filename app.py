from utils import *
from sql import SQL
from cos import COS
from nosql import NoSQL

app = Flask(__name__)
CORS(app)

# ---------------------------------------------------------------
#                   Error handling and Test
# ---------------------------------------------------------------

@app.errorhandler(400)
def bad_request(error):
    return make_response(jsonify({"response": "Bad request"}), 400)


@app.errorhandler(404)
def not_found(error):
    return make_response(jsonify({"response": "Not found"}), 404)


@app.route("/", methods=["GET"])
def test_app():
    return jsonify({"success": "true"})


# ---------------------------------------------------------------
#                           SQL DB Endpoints
# ---------------------------------------------------------------

@app.route("/database/sql/create", methods=["POST"])
def sql_create():

    # Request body
    file = request.files['file']

    with open(file.filename, 'r') as fp:
        txt_file = fp.read().splitlines()

    try:
        response = py_sql.create(txt_file)
    except Exception as e:
        print(e)
        return make_response(jsonify({"response": str(e)}), 500)
    else:
        return make_response(jsonify({"response": "success"}), 200)
    

@app.route("/database/sql/update", methods=["POST"])
def sql_update():

    # Request body
    table_name = request.json.get("table_name")
    jsonified_df = request.json.get("jsonified_df")
    update_targets = request.json.get("update_targets")

    try:
        df = pd.DataFrame(jsonified_df)
    except ValueError:
        df = pd.DataFrame(jsonified_df, index=[0])

    try:
        response = py_sql.update(df, table_name, update_targets)
    except Exception as e:
        print(e)
        return make_response(jsonify({"response": str(e)}), 500)
    else:
        return make_response(jsonify({"response": "success"}), 200)


@app.route("/database/sql/read", methods=["POST"])
def sql_read():

    query = request.json.get("query")

    try:
        response = py_sql.read(query)
    except Exception as e:
        print(e)
        return make_response(jsonify({"response": str(e)}), 500)
    else:
        return make_response(jsonify({"response": response}), 200)


@app.route("/database/sql/delete", methods=["POST"])
def sql_delete():

    # Request body
    table_name = request.json.get("table_name")
    params = request.json.get("params")
    
    try:
        py_sql.delete(table_name)
    except Exception as e:
        print(e)
        return make_response(jsonify({"response": str(e)}), 500)
    else:
        return make_response(jsonify({"response": response}), 200)  

# ---------------------------------------------------------------
#                      NoSQL Endpoints
# ---------------------------------------------------------------

@app.route("/database/nosql/sequence", methods=["GET"])
def get_sequence_id():

    try:
        # Get new unique customer ID
        id =  py_nosql.get_sequence()
    except Exception as e:
        print(e)
        return make_response(jsonify({"response": str(e)}), 500)
    else:
        return make_response(jsonify({"response": id}), 200)  

@app.route("/database/nosql/collection/update", methods=["POST"])
def update_collection():

    # Request body
    col_name = request.json.get("col_name")
    document = request.json.get("document")

    try:
         py_nosql.update_collection(col_name, document)
    except Exception as e:
        print(e)
        return make_response(jsonify({"response": str(e)}), 500)
    else:
        return make_response(jsonify({"response": "collection updated"}), 200)     

@app.route("/database/nosql/collection/document", methods=["GET"])
def get_document():

    # Request body
    id = request.json.get("id")
    col_name = request.json.get("col_name")

    try:
        db =  py_nosql.get_database()
        collection = db[col_name]
        document = list(collection.find({"_id":id}))
    except Exception as e:
        print(e)
        return make_response(jsonify({"response": str(e)}), 500)
    else:
        return make_response(jsonify({"response": document}), 200)   


# ---------------------------------------------------------------
#                      Object Storage Functions
# ---------------------------------------------------------------

# Functionality tested
@app.route("/database/cos/bucket/create", methods=["GET"])
def create_bucket():
    bucket_name = request.json.get("bucket_name")
    
    try:
        buckets = py_cos.get_buckets()
    except Exception as e:
        print(e)
        return make_response(jsonify({"response": str(e)}), 500)
    else:
        if bucket_name in buckets:
            return make_response(jsonify({"response": "bucket exists"}), 500) 
        else:
            try:
                py_cos.create_bucket(bucket_name=bucket_name)
            except Exception as e:
                print(e)
                return make_response(jsonify({"response": str(e)}), 500)
            else:
                return make_response(jsonify({"response": "bucket created"}), 200) 


# Functionality tested
@app.route("/database/cos/bucket/delete", methods=["GET"])
def bucket_delete():
    bucket_name = request.json.get("bucket_name")

    buckets = py_cos.get_buckets()

    if bucket_name not in buckets:
        return make_response(jsonify({"response": "bucket doesn't exist"}), 200)
    else:
        try:
            bucket_contents = py_cos.get_bucket_contents(bucket_name)
        except Exception as e:
            print(e)
            return make_response(jsonify({"response": str(e)}), 500)
        else:
            if bucket_contents:

                # empty bucket
                for filename in bucket_contents:
                    try:
                        py_cos.delete_file_cos(bucket_name, filename)
                    except Exception as e:
                        print(e)
                        return make_response(jsonify({"response": str(e)}), 500)
                    else:
                        print(f"File {filename} from bucket {bucket_name} deleted")

                # delete bucket
                try:
                    py_cos.delete_bucket(bucket_name)
                except Exception as e:
                    print(e)
                    return make_response(jsonify({"response": str(e)}), 500)
                else:
                    return make_response(jsonify({"response": "Success"}), 200)

            else:
                try:
                    py_cos.delete_bucket(bucket_name)
                except Exception as e:
                    print(e)
                    return make_response(jsonify({"response": str(e)}), 500)
                else:
                    return make_response(jsonify({"response": "bucket delete successful"}), 200)


# Functionality tested
@app.route("/database/cos/bucket/empty", methods=["GET"])
def empty_bucket():
    bucket_name = request.json.get("bucket_name")

    buckets = py_cos.get_buckets()

    if bucket_name not in buckets:
        return make_response(jsonify({"response": "bucket doesn't exist"}), 200)
    else:
        try:
            bucket_contents = py_cos.get_bucket_contents(bucket_name)
        except Exception as e:
            print(e)
            return make_response(jsonify({"response": str(e)}), 500)
        else:
            if bucket_contents:

                # empty bucket
                for filename in bucket_contents:
                    try:
                        py_cos.delete_file_cos(bucket_name, filename)
                    except Exception as e:
                        print(e)
                        return make_response(jsonify({"response": str(e)}), 500)
                    else:
                        print(f"File {filename} from bucket {bucket_name} deleted")
                return make_response(jsonify({"response": "emptied bucket"}), 200)
            else:
                return make_response(jsonify({"response": "bucket already empty"}), 200)


# Functionality tested
@app.route("/database/cos/bucket/objects/upload_object", methods=["POST"])
def push_to_cos():
    file = request.files['file']
    bucket_name = request.form.get("bucket_name")

    try:
        py_cos.upload_file_cos(bucket_name, file.filename, file)
    except Exception as e:
        print(e)
        return make_response(jsonify({"response": str(e)}), 500)
    else:
        return make_response(jsonify({"response":{
            "status":"successful",
            "item_name":file.filename,
            "bucket_name":bucket_name}
        }), 200)


# Functionality tested
@app.route("/database/cos/bucket/objects/retrieve_object", methods=["GET"])
def get_from_cos():

    item_name = request.json.get("item_name")
    bucket_name = request.json.get("bucket_name")

    print(f"Bucket_name: {bucket_name}")

    try:
        buckets = py_cos.get_buckets()
    except Exception as e:
        print(e)
        return make_response(jsonify({"response": str(e)}), 500)
    else:
        if bucket_name in buckets:
            try:
                item = py_cos.get_item(bucket_name, item_name)
            except Exception as e:
                print(e)
                return make_response(jsonify({"response": str(e)}), 500)
            else:
                print("success")
                return make_response(jsonify({"response": item}), 200)
        else:
            return make_response(jsonify({"response": "bucket empty"}), 500)


# Functionality tested
@app.route("/database/cos/bucket/objects/delete_object", methods=["GET"])
def delete_from_cos():
    item_name = request.json.get("item_name")
    bucket_name = request.json.get("bucket_name")

    buckets = py_cos.get_buckets()
    if bucket_name not in buckets:
        return make_response({"response": "bucket doesn't exist"}, 200)
    else:
        try:
            py_cos.delete_file_cos(bucket_name, item_name)
        except Exception as e:
            print(e)
            return make_response(jsonify({"response": str(e)}), 500)
        else:
            return make_response(jsonify({"response": f"File {item_name} from bucket {bucket_name} deleted"}), 200)


# Functionality tested
@app.route("/database/cos/bucket/objects/list_objects", methods=["GET"])
def list_contents_from_cos():
    
    # Request body
    prefix = request.json.get("prefix")
    bucket_name = request.json.get("bucket_name")

    try:
        bucket_contents = py_cos.get_bucket_contents(bucket_name, prefix)
    except Exception as e:
        print(e)
        return make_response(jsonify({"response": str(e)}), 500)
    else:
        return make_response(jsonify({"response": bucket_contents}), 200)
    

# Functionality tested
@app.route("/database/cos/bucket/list_buckets", methods=["GET"])
def list_buckets_from_cos():
    try:
        buckets = py_cos.get_buckets()
    except Exception as e:
        print(e)
        return make_response(jsonify({"response": str(e)}), 500)
    else:
        return make_response(jsonify({"response": buckets}), 200)
# ---------------------------------------------------------------
#                           Entry Point
# ---------------------------------------------------------------


if __name__ == '__main__':
    py_sql = SQL()
    py_cos = COS()
    py_nosql = NoSQL()
    app.run(host='0.0.0.0', port=8080, debug=True)
