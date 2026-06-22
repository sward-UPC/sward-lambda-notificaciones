import json
import os
from contextlib import contextmanager


def _get_db_url() -> str:
    url = os.environ.get("DATABASE_URL", "")
    if url:
        return url

    host = os.environ.get("DATABASE_HOST", "")
    port = os.environ.get("DATABASE_PORT", "5432")
    name = os.environ.get("DATABASE_NAME", "")
    secret_arn = os.environ.get("DB_SECRET_ARN", "")

    if not (host and secret_arn):
        raise RuntimeError(
            f"Variables de entorno DB incompletas: DATABASE_HOST={host!r}, DB_SECRET_ARN={secret_arn!r}"
        )

    import boto3

    client = boto3.client(
        "secretsmanager", region_name=os.environ.get("AWS_REGION", "us-east-1")
    )
    secret = json.loads(client.get_secret_value(SecretId=secret_arn)["SecretString"])
    username = secret.get("username", "")
    password = secret.get("password", "")
    return f"postgresql://{username}:{password}@{host}:{port}/{name}"


@contextmanager
def get_connection():
    try:
        import psycopg2
    except ImportError:
        raise RuntimeError(
            "psycopg2 no disponible. Incluirlo en requirements.txt del Lambda."
        )

    url = _get_db_url()
    conn = psycopg2.connect(url)
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
