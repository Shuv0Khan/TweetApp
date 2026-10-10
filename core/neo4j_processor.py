from neo4j import GraphDatabase

from utils import env_config


class Neo4jProcessor:
    def __init__(self, uri=None, user=None, pwd=None):
        # Connection settings default to NEO4J_URI / NEO4J_USER / NEO4J_PASSWORD
        self.__uri = uri or env_config.get('NEO4J_URI', 'bolt://localhost:7687')
        self.__user = user or env_config.get('NEO4J_USER', 'neo4j')
        self.__pwd = pwd or env_config.require('NEO4J_PASSWORD')
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