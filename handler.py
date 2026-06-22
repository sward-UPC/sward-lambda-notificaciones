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
        logger.warning(
            "FeedbackRegistrado sin estudiante_id, se omite | payload=%s", payload
        )
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
    logger.info(
        "Notificación de feedback creada | estudiante=%s | event_id=%s",
        estudiante_id,
        event_id,
    )
    return True


_SELECT_ADMINS = """
SELECT ur.user_id
FROM user_roles ur
JOIN roles r ON r.id = ur.role_id
WHERE r.nombre = 'administrador'
"""


def _crear_notificacion_usuario_registrado(payload: dict, event_id: str) -> bool:
    """Notifica a TODOS los administradores que se registró un usuario nuevo.
    Idempotente por event_id (una sola vez aunque haya varios admins)."""
    correo = payload.get("correo") or "Un usuario"
    rol = payload.get("rol") or "usuario"
    notif_payload = {"usuario_id": payload.get("usuario_id"), "rol": rol}

    import psycopg2.extras

    with get_connection() as conn:
        if ya_procesado(conn, event_id):
            conn.commit()
            logger.info("Evento ya procesado, se omite | event_id=%s", event_id)
            return False
        with conn.cursor() as cur:
            cur.execute(_SELECT_ADMINS)
            admin_ids = [row[0] for row in cur.fetchall()]
            for admin_id in admin_ids:
                cur.execute(
                    _INSERT_NOTIF,
                    (
                        str(uuid.uuid4()),
                        str(admin_id),
                        "sistema",
                        "Nuevo usuario registrado",
                        f"{correo} se registró como {rol}.",
                        psycopg2.extras.Json(notif_payload),
                        datetime.now(timezone.utc),
                    ),
                )
        conn.commit()
    logger.info(
        "Notificaciones de alta creadas | admins=%d | event_id=%s",
        len(admin_ids),
        event_id,
    )
    return bool(admin_ids)


def _docente_del_curso(curso_id: str) -> str | None:
    """Resuelve el docente_id de un curso desde cursos_recursos_db."""
    with get_connection("CURSOS_") as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT docente_id FROM courses WHERE id = %s", (curso_id,))
            row = cur.fetchone()
            return str(row[0]) if row and row[0] else None


def _crear_notificacion_alerta(payload: dict, event_id: str) -> bool:
    """Notifica al DOCENTE del curso que un alumno entró en riesgo.
    Resuelve el docente vía cursos_db; idempotente por event_id."""
    curso_id = payload.get("curso_id")
    if not curso_id:
        logger.warning("AlertaCreada sin curso_id, se omite | payload=%s", payload)
        return False

    docente_id = _docente_del_curso(curso_id)
    if not docente_id:
        logger.info("Curso sin docente asignado, se omite | curso=%s", curso_id)
        return False

    nivel = payload.get("nivel_riesgo") or "alto"
    mensaje = (payload.get("mensaje") or "").strip()
    notif_payload = {
        "estudiante_id": payload.get("estudiante_id"),
        "curso_id": curso_id,
        "nivel_riesgo": nivel,
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
                    docente_id,
                    "alerta",
                    f"Alumno en riesgo {nivel}",
                    mensaje,
                    psycopg2.extras.Json(notif_payload),
                    datetime.now(timezone.utc),
                ),
            )
        conn.commit()
    logger.info(
        "Notificación de alerta creada | docente=%s | curso=%s | event_id=%s",
        docente_id,
        curso_id,
        event_id,
    )
    return True


def _crear_notificacion_logro(payload: dict, event_id: str) -> bool:
    """Felicita al alumno por un hito (racha o recursos). Idempotente por
    event_id determinístico (un hito notifica una sola vez)."""
    estudiante_id = payload.get("estudiante_id")
    if not estudiante_id:
        logger.warning(
            "LogroDesbloqueado sin estudiante_id, se omite | payload=%s", payload
        )
        return False

    tipo = payload.get("tipo") or "racha"
    valor = payload.get("valor") or 0
    if tipo == "racha":
        titulo = "¡Racha desbloqueada!"
        mensaje = f"Llevas {valor} días seguidos estudiando. ¡Sigue así!"
    else:
        titulo = "¡Logro desbloqueado!"
        mensaje = f"Completaste {valor} recursos. ¡Gran trabajo!"

    notif_payload = {"logro": tipo, "valor": valor}

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
                    str(estudiante_id),
                    "logro",
                    titulo,
                    mensaje,
                    psycopg2.extras.Json(notif_payload),
                    datetime.now(timezone.utc),
                ),
            )
        conn.commit()
    logger.info(
        "Notificación de logro creada | estudiante=%s | tipo=%s | valor=%s | event_id=%s",
        estudiante_id,
        tipo,
        valor,
        event_id,
    )
    return True


# Despacho por detail-type del evento.
_HANDLERS = {
    "sward.trazabilidad.FeedbackRegistrado": _crear_notificacion_feedback,
    "sward.usuarios.UsuarioRegistrado": _crear_notificacion_usuario_registrado,
    "sward.alertas.AlertaCreada": _crear_notificacion_alerta,
    "sward.trazabilidad.LogroDesbloqueado": _crear_notificacion_logro,
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
            detail_type = (
                body.get("event_type")
                or body.get("detail_type")
                or payload.get("event_type")
            )
            handler = _HANDLERS.get(detail_type)
            if handler is None:
                logger.info(
                    "Evento sin handler, se omite | detail_type=%s", detail_type
                )
                continue
            if handler(payload, event_id):
                creadas += 1
        except Exception:
            logger.exception("Error procesando record SQS | record=%s", record)
            raise  # deja que SQS reintente / mande a DLQ
    return {"notificaciones_creadas": creadas}


lambda_handler = handle_sqs_message
