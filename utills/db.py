import psycopg


def create_pg_conn(host: str, user: str, password: str, dbname: str, port: int=5432):
    connect=psycopg.connect(host=host, user=user, password=password, dbname=dbname, port=port)
    return connect


class TableMetadataMismatchError(Exception):
    """Raised when source and target table metadata do not match."""
    pass