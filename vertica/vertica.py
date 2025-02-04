import vertica_python
from vertica_python import errors
import os
from dotenv import load_dotenv

load_dotenv()

def get_vertica_connection():
    try:
        vertica_connection_string = os.getenv("VERTICA_CONNECTION_STRING")
        if not vertica_connection_string:
            raise ValueError("VERTICA_CONNECTION_STRING is not set in the .env file.")
        
        conn_info = {}
        for part in vertica_connection_string.split(";"):
            key, value = part.split("=")
            conn_info[key.strip()] = value.strip()

        conn_info.setdefault("tlsmode", "disable")
        connection = vertica_python.connect(**conn_info)
        return connection

    except Exception as e:
        print(f"Error while connecting to Vertica: {e}")
        return None


def execute_vertica_query(vertica_connection, query):
    try:
        with vertica_connection.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchall()
            return result
    except errors.MissingColumn as e:
        return -1
    except errors.QueryError as e:
        if "does not exist" in str(e):
            return -1
        else:
            print(f"Error executing query: {e}")
    except Exception as e:
        print(f"Error executing query: {e}")
        return None