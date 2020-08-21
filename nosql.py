from utils import *

class NoSQL():

    def __init__(self):
        """
            Initializes NoSQL class and retrieves associated credentials
        """
        self._mongo_cli = MongoClient(
            mongo_composed,
            ssl = True,
            ssl_ca_certs = "./tmpCert.crt")        

    def get_database(self):
        """
            Retrieves mongodb object for specified database name

            Parameters:
                db_name: <str>
                        name of target database
        """
        db = self._mongo_cli.get_database(nosql_db_name)
        return db

    def update_collection(self, col_name, document):
        """
            Updates target database collection for target database

            1. Retrieves mongodb object
            2. Retrieve list of avaliable collections in target db
            3. If collection exists retrieve target collection, else create a new one 
               and retrieve it
            4. If document exists update it, else create new document

            arameters:
                db_name: <str>
                         name of target database

                col_name: <str>
                          name of targeet mongodb collection

                document: <list>
                          list of documents retrieved from target collection
        """
        # Get database
        db = self._mongo_cli.get_database(db_name)

        # Check if collection exists
        collection_names = db.list_collection_names()

        if col_name not in collection_names:
            # Create collection
            db.create_collection(col_name)

        collection = db.get_collection(col_name)
        collection.insert_one(document)

    def get_sequence(self):
        """
            Retrieves last updated value in the sequence collection

            Parameters:
                        db_name: <str>
                        name of target database
        """
        db = self.get_database()
        document = db.sequences.find_one_and_update({"_id": "unique_ids"}, {"$inc": {"value": 1}}, upsert=True, return_document=True)
        id = document["value"]
        return id

if __name__ == '__main__':
    nosql = NoSQL()