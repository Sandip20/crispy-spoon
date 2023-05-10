# pylint: disable=redefined-builtin
"""
This module contains a Python class named Mongo that provides a wrapper around the pymongo library
for connecting to a MongoDB instance and performing database operations.

Example usage:

    # Create a new instance of the Mongo class
    mongo = Mongo(url='mongodb://localhost:27017/', db='mydb')

    # Insert a single document into the collection
    data = {'name': 'John Doe', 'age': 30}
    mongo.insert_one(data)

    # Insert multiple documents into the collection
    data = [{'name': 'John Doe', 'age': 30}, {'name': 'Jane Smith', 'age': 25}]
    mongo.insert_many(data)

    # Find a single document in the collection
    filter = {'name': 'John Doe'}
    document = mongo.find_one(filter)

    # Find multiple documents in the collection
    filter = {'age': {'$gt': 25}}
    documents = mongo.find_many(filter)

    # Update a single document in the collection
    filter = {'name': 'John Doe'}
    update = {'age': 31}
    mongo.update_one(filter, update)

    # Update multiple documents in the collection
    filter = {'age': {'$lt': 30}}
    update = {'age': 30}
    mongo.update_many(filter, update)

    # Delete a single document from the collection
    filter = {'name': 'John Doe'}
    mongo.delete_one(filter)
"""
from typing import Dict, Any, List, Optional, Tuple
from pymongo import MongoClient
import certifi
ca = certifi.where()


class Mongo:
    """
    This class is responsible for connecting to a MongoDB instance 
    and performing database operations.

    Attributes:
        client (MongoClient): A MongoClient instance that represents the database client.
        db (str): A string that represents the name of the database.
    """

    def __init__(self, url: str, db_name: str, is_ca_required: bool = False) -> None:
        """
        Initializes a new instance of the Mongo class.

        Args:
            url (str): A string that represents the MongoDB connection URL.
            db (str): A string that represents the name of the database.
        """
        if is_ca_required:
            self.client = MongoClient(url, tlsCAFile=ca)
        else:
            self.client = MongoClient(url)
        self.db = self.client[db_name]

    def insert_one(self, data: Dict[str, Any], collection: str) -> None:
        """
        Inserts a single document into the collection.

        Args:
            data (Dict[str, Any]): A dictionary that represents the data to be inserted.
        """
        collection = self.db[collection]
        collection.insert_one(data)

    def insert_many(self, data: Dict[str, Any], collection: str) -> None:
        """
        Inserts multiple documents into the collection.

        Args:
            data (List[Dict[str, Any]]): A list of dictionaries that represent the data to be inserted.
        """
        collection = self.db[collection]
        collection.insert_many(data)

    def find_one(self, filter: Dict[str, Any], collection: str,
                 sort: Optional[List[Tuple[str, int]]] = None) -> Dict[str, Any]:
        """
        Finds a single document in the collection that matches the specified filter.

        Args:
            filter (Dict[str, Any]): A dictionary that represents the filter criteria.
            collection (str): The name of the collection to search in.
            sort (Optional[List[Tuple[str, int]]]): A list of tuples specifying the sorting criteria.

        Returns:
            Dict[str, Any]: A dictionary that represents the matching document, or None if no documents match.
        """
        collection = self.db[collection]
        
        return collection.find_one(filter,sort=sort) if len(filter)>0 else collection.find_one(sort=sort)

    def find_many(self, filter: Dict[str, Any], collection: str,sort: Optional[List[Tuple[str, int]]] = None,limit=1000000) -> List[Dict[str, Any]]:
        """
        Finds multiple documents in the collection that match the specified filter.

        Args:
            filter (Dict[str, Any]): A dictionary that represents the filter criteria.

        Returns:
            List[Dict[str, Any]]: A list of dictionaries that represent the matching documents, or an empty list if no documents match.
        """

        collection = self.db[collection]

        result = list(collection.find(filter,sort=sort,limit=limit))

        return result

    def update_one(self, filter: Dict[str, Any], update: Dict[str, Any], collection: str) -> None:
        """
        Updates a single document in the collection that matches the specified filter.

        Args:
            filter (Dict[str, Any]): A dictionary that represents the filter criteria.
            update (Dict[str, Any]): A dictionary that represents the update criteria.
        """
        collection = self.db[collection]
        collection.update_one(filter, {"$set": update})

    def update_many(self, filter: Dict[str, Any], update: Dict[str, Any], collection: str) -> None:
        """
        Updates multiple documents in the collection that match the specified filter.

        Args:
            filter (Dict[str, Any]): A dictionary that represents the filter criteria.
            update (Dict[str, Any]): A dictionary that represents the update criteria.
        """
        collection = self.db[collection]
        collection.update_many(filter, {"$set": update})

    def delete_one(self, filter: Dict[str, Any], collection) -> None:
        """
        Deletes a single document from the collection that matches the specified filter.

        Args:
            filter (Dict[str, Any]): A dictionary that represents the filter criteria.
        """
        collection = self.db[collection]
        collection.delete_one(filter)

    def delete_many(self, filter: Dict[str, Any], collection) -> None:
        """
        Deletes a multiple document from the collection that matches the specified filter.

        Args:
            filter (Dict[str, Any]): A dictionary that represents the filter criteria.
        """
        collection = self.db[collection]
        collection.delete_many(filter)

    def aggregate(self, query: Dict[str, Any], collection: str) -> List[Dict[str, Any]]:
        """
        Args:
        query:Dict[str, Any]
        collection:str

        Returns:
        List[Dict[str, Any]]
        """
        collection = self.db[collection]
        return list(collection.aggregate(query))

    def bulk_write(self, bulk_operations, collection: str):
        """
        Args:
        bulk_operations:[]
        collection:str

        Returns:
        results

        """
        collection = self.db[collection]
        return collection.bulk_write(bulk_operations)
