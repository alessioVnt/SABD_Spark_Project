import sys
from pymongo import MongoClient  
import json
import datetime


def mongo_put(my_dict, collection_name, db_name):
    client = MongoClient("mongodb://mongo_server:27017")
    db = client[db_name]
    collection = db[collection_name]

    query_result = {
        "query_timestamp": str(datetime.datetime.now()),
        "result": my_dict
    }
    collection.insert(query_result)


def generate_list_from_json():

    my_list = []

    for line in sys.stdin:
        my_list.append(json.loads(line))

    return my_list


def get_param():


    db_name = str(sys.argv[1])
    collection_name = str(sys.argv[2])

    return collection_name, db_name


def main():

    collection_name, db_name = get_param()
    my_list = generate_list_from_json()
    mongo_put(my_list, collection_name, db_name)

    return 0


if __name__ == '__main__':
    main()
