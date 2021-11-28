from neo4j import GraphDatabase


class Neo4jProcessor:
    def __init__(self, uri='bolt://localhost:7687', user='neo4j', pwd='lab@2021'):
        self.__uri = uri
        self.__user = user
        self.__pwd = pwd
        self.__driver = None
        self.__session = None
        try:
            self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
            self.__session = self.__driver.session()
        except Exception as e:
            print("Failed to create the driver:", e)

    def __enter__(self):
        return self

    def query(self, query, db=None):
        assert self.__driver is not None, "Driver not initialized!"
        response = None
        try:
            if self.__session is None:
                self.__session = self.__driver.session(database=db) if db is not None else self.__driver.session()
            response = list(self.__session.run(query))
        except Exception as e:
            print("Query failed:", e)
        return response

    def __exit__(self, exc_type, exc_value, traceback):
        if self.__session is not None:
            self.__session.close()
        if self.__driver is not None:
            self.__driver.close()