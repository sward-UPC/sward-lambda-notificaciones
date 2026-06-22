"""Lambda notificaciones — consume eventos de EventBridge (vía SQS) y crea
notificaciones en la BD de ms-usuarios.

Trigger: EventBridge rule → SQS → esta Lambda.
Hoy procesa `sward.trazabilidad.FeedbackRegistrado` (retro docente→estudiante).
"""

import json
import uuid
from datetime import datetime, timezone

from lib.db_client import get_connection
from lib.idempotency import ya_procesado
from lib.logger import setup_logger

logger = setup_logger(__name__)

_INSERT_NOTIF = """
INSERT INTO notificaciones (id, destinatario_id, tipo, titulo, mensaje, payload, leida, created_at)
VALUES (%s, %s, %s, %s, %s, %s, FALSE, %s)
"""


def _payload_de(record_body: dict) -> dict:
    """Extrae el payload del evento desde el cuerpo del mensaje SQS.

    EventBridge envía `$.detail` (el envelope de sward_shared) como cuerpo. El
    envelope trae los datos del evento en `payload_json` (string JSON)."""
    if "payload_json" in record_body:
        return json.loads(record_body["payload_json"])
    if "detail" in record_body:
        return record_body["detail"]
    return record_body


def _event_id_de(record_body: dict, payload: dict) -> str:
    return (
        record_body.get("event_id")
        or payload.get("event_id")
        or payload.get("feedback_id")
        or str(uuid.uuid4())
    )


def _crear_notificacion_feedback(payload: dict, event_id: str) -> bool:
    """Crea la notificación de retro para el estudiante. Idempotente por event_id.

    Returns True si insertó, False si ya estaba procesado o faltan datos."""
    estudiante_id = payload.get("estudiante_id")
    if not estudiante_id:
        logger.warning("FeedbackRegistrado sin estudiante_id, se omite | payload=%s", payload)
        return False

    mensaje = (payload.get("mensaje") or "").strip()
    notif_payload = {
        "feedback_id": payload.get("feedback_id"),
        "curso_id": payload.get("curso_id"),
        "docente_id": payload.get("docente_id"),
        "tipo_feedback": payload.get("tipo"),
    }

    import psycopg2.extras

    with get_connection() as conn:
        if ya_procesado(conn, event_id):
            conn.commit()
            logger.info("Evento ya procesado, se omite | event_id=%s", event_id)
            return False
        with conn.cursor() as cur:
            cur.execute(
                _INSERT_NOTIF,
                (
                    str(uuid.uuid4()),
                    estudiante_id,
                    "feedback",
                    "Retroalimentación de tu docente",
                    mensaje,
                    psycopg2.extras.Json(notif_payload),
                    datetime.now(timezone.utc),
                ),
            )
        conn.commit()
    logger.info("Notificación de feedback creada | estudiante=%s | event_id=%s", estudiante_id, event_id)
    return True


# Despacho por detail-type del evento.
_HANDLERS = {
    "sward.trazabilidad.FeedbackRegistrado": _crear_notificacion_feedback,
}


def handle_sqs_message(event: dict, context) -> dict:
    records = event.get("Records", [])
    creadas = 0
    for record in records:
        try:
            body = json.loads(record["body"])
            payload = _payload_de(body)
            event_id = _event_id_de(body, payload)
            # detail-type: viene en el envelope; si no, inferimos por contenido.
            detail_type = body.get("event_type") or body.get("detail_type") or payload.get("event_type")
            handler = _HANDLERS.get(detail_type, _crear_notificacion_feedback)
            if handler(payload, event_id):
                creadas += 1
        except Exception:
            logger.exception("Error procesando record SQS | record=%s", record)
            raise  # deja que SQS reintente / mande a DLQ
    return {"notificaciones_creadas": creadas}


lambda_handler = handle_sqs_message
