import json
import os
from contextlib import contextmanager


def _get_db_url(prefix: str = "") -> str:
    """Construye la URL de conexión. `prefix` permite una 2ª BD (ej. "CURSOS_").

    Sin prefijo usa DATABASE_* / DB_SECRET_ARN (BD de usuarios, por defecto).
    Con prefijo usa {PREFIX}DATABASE_* / {PREFIX}DB_SECRET_ARN.
    """
    url = os.environ.get(f"{prefix}DATABASE_URL", "")
    if url:
        return url

    host = os.environ.get(f"{prefix}DATABASE_HOST", "")
    port = os.environ.get(f"{prefix}DATABASE_PORT", "5432")
    name = os.environ.get(f"{prefix}DATABASE_NAME", "")
    secret_arn = os.environ.get(f"{prefix}DB_SECRET_ARN", "")

    if not (host and secret_arn):
        raise RuntimeError(
            f"Variables de entorno DB incompletas para prefijo {prefix!r}: "
            f"host={host!r}, secret_arn={secret_arn!r}"
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
def get_connection(prefix: str = ""):
    try:
        import psycopg2
    except ImportError:
        raise RuntimeError(
            "psycopg2 no disponible. Incluirlo en requirements.txt del Lambda."
        )

    url = _get_db_url(prefix)
    conn = psycopg2.connect(url)
    try:
        yield conn
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
