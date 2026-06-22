"""Deduplicación de eventos por event_id para entrega at-least-once (SQS/EventBridge).

Garantiza idempotencia: un mismo event_id solo se procesa una vez aunque el
broker reentregue el evento. El INSERT de dedup debe ejecutarse dentro de la
MISMA transacción que la lógica de negocio para que ambos sean atómicos.
"""

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS processed_events (
    event_id TEXT PRIMARY KEY,
    processed_at TIMESTAMPTZ DEFAULT now()
)
"""

_INSERT_SQL = """
INSERT INTO processed_events (event_id)
VALUES (%s)
ON CONFLICT (event_id) DO NOTHING
"""


def ya_procesado(conn, event_id: str) -> bool:
    """Marca event_id como procesado de forma atómica vía ON CONFLICT DO NOTHING.

    No hace commit: la transacción la cierra el llamador junto con la lógica
    de negocio, garantizando atomicidad entre el dedup y el INSERT.

    Returns:
        True  -> el evento ya había sido procesado antes (se debe omitir).
        False -> es la primera vez que se ve este event_id (se debe procesar).
    """
    with conn.cursor() as cur:
        cur.execute(_CREATE_TABLE_SQL)
        cur.execute(_INSERT_SQL, (event_id,))
        # rowcount == 0 cuando ON CONFLICT impidió la inserción (ya existía).
        return cur.rowcount == 0
