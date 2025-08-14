import psycopg


def create_pg_cursor(host: str, user: str, password: str, dbname: str, port: int=5432):
    connect=psycopg.connect(host=host, user=user, password=password, dbname=dbname, port=port)
    cursor=connect.cursor()
    return cursor


class TableMetadataMismatchError(Exception):
    """Raised when source and target table metadata do not match."""
    pass