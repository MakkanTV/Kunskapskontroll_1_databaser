import pandas as pd
import json
import streamlit as st
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi


PWD = open("mongodb.pwd", "r").read().strip()

uri = f"mongodb+srv://Kunskapskontoll_1:maxdb@cluster0.vzhgs.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

client = MongoClient(uri, server_api=ServerApi('1'))

try:
    client.admin.command('ping')
    print("pinged your deploment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)


database = client["Northwind"]
collection = database["Products"]

products = pd.read_csv("products.csv", index_col=False)
suppliers = pd.read_json("suppliers.json")

st.header("Hello")



products["SupplierDetails"] = \
products.apply(
    lambda x: json.loads(
        suppliers.query(f"SupplierID == {x.SupplierID}")
        .to_json(orient="records"))[0],
        axis = 1)

products.drop("SupplierID", axis = 1, inplace=True)

products_data = json.loads(products.to_json(orient="records"))

collection.delete_many({})
collection.insert_many(products_data)

collection.update_many({},
[
    {
        "$addFields": {
            "SupplierID": "$SupplierDetails.SupplierID",
            "CompanyName": "$SupplierDetails.CompanyName",
            "ContactName": "$SupplierDetails.ContactName",
            "Phone": "$SupplierDetails.Phone"
        }
        
    },
    {
        "$project": {
            "SupplierDetails": 0
        }
    }
]
)

    

query = {
    "$expr": {
        "$gt": [
            "$ReorderLevel",
            {"$sum": ["$UnitsInStock", "$UnitsOnOrder"] }
        ]
    }
}

products_to_order = list(collection.find(query))

if products_to_order:
    df = pd.DataFrame(products_to_order)

    st.dataframe(df)
else:
    st.write("Inga Produkter behöver beställas")

data_df = pd.DataFrame(
    {
        "price": [20, 950, 250, 500],
    }
)





