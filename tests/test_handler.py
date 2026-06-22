import json

import handler


def test_payload_de_desde_payload_json():
    """El envelope de sward_shared trae los datos en payload_json (string)."""
    inner = {"estudiante_id": "e1", "mensaje": "hola", "tipo": "encouragement"}
    body = {"payload_json": json.dumps(inner)}
    assert handler._payload_de(body) == inner


def test_payload_de_desde_detail():
    body = {"detail": {"estudiante_id": "e2"}}
    assert handler._payload_de(body) == {"estudiante_id": "e2"}


def test_payload_de_plano():
    body = {"estudiante_id": "e3"}
    assert handler._payload_de(body) == {"estudiante_id": "e3"}


def test_event_id_prefiere_event_id():
    body = {"event_id": "evt-1"}
    payload = {"feedback_id": "fb-1"}
    assert handler._event_id_de(body, payload) == "evt-1"


def test_event_id_cae_a_feedback_id():
    body = {}
    payload = {"feedback_id": "fb-9"}
    assert handler._event_id_de(body, payload) == "fb-9"
