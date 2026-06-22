# sward-lambda-notificaciones

Función AWS Lambda (Python 3.11) de la plataforma **SWARD**. Consume eventos de dominio desde
**Amazon EventBridge** (entregados a través de una cola **Amazon SQS**) y **persiste
notificaciones** en la base de datos de `ms-usuarios`, para que estudiantes, docentes y
administradores las vean en su panel.

Es un consumidor dirigido por eventos: EventBridge enruta los eventos relevantes a una regla,
la regla los deposita en SQS, y SQS dispara esta Lambda (event source mapping). Cada record se
procesa de forma **idempotente**; si algo falla, el record se re-lanza para que SQS reintente
o lo derive a la DLQ.

```
Microservicios SWARD ──put_events──▶ EventBridge ──regla──▶ SQS ──trigger──▶ Lambda ──INSERT──▶ BD usuarios
```

## Qué hace

Por cada mensaje SQS la Lambda:

1. Parsea el cuerpo del mensaje y extrae el **payload** del evento (del envelope de
   `sward_shared`: campo `payload_json`, o `detail` de EventBridge, o el cuerpo plano).
2. Determina el **`event_id`** para deduplicación y el **detail-type** del evento.
3. Despacha al handler correspondiente según el detail-type (mapa `_HANDLERS`).
4. El handler verifica idempotencia, aplica reglas (p. ej. preferencias del usuario) e
   **inserta la notificación** en la tabla `notificaciones`.

## Eventos que consume (detail-types)

| Detail-type | Origen | Destinatario | Notificación |
|---|---|---|---|
| `sward.trazabilidad.FeedbackRegistrado` | Trazabilidad | Estudiante | Retroalimentación del docente |
| `sward.usuarios.UsuarioRegistrado` | Usuarios | Todos los administradores | Alta de un usuario nuevo |
| `sward.alertas.AlertaCreada` | Alertas | Docente del curso | Alumno en riesgo |
| `sward.trazabilidad.LogroDesbloqueado` | Trazabilidad | Estudiante | Racha / logro desbloqueado |

Un detail-type sin handler registrado se ignora (se loguea y se hace ack), no provoca error.

## Bases de datos

La Lambda se conecta a **dos** bases de datos PostgreSQL mediante un patrón de prefijo de
variables de entorno (`lib/db_client.py`):

- **BD de usuarios** (por defecto, sin prefijo): donde se persisten las notificaciones
  (`notificaciones`), se consultan administradores (`user_roles` / `roles`), la preferencia
  `notif_logros` (`users`) y la tabla de deduplicación (`processed_events`).
- **BD de cursos** (prefijo `CURSOS_`): solo lectura, para resolver el `docente_id` de un curso
  (`courses`) al procesar `AlertaCreada`.

Cada base se configura con su propio juego de variables. Sin prefijo para usuarios; con prefijo
`CURSOS_` para cursos.

## Idempotencia

SQS/EventBridge entregan **at-least-once**: un mismo evento puede llegar más de una vez. Para no
duplicar notificaciones, cada evento se deduplica por **`event_id`** (`lib/idempotency.py`):

- Se inserta el `event_id` en `processed_events` con `INSERT ... ON CONFLICT (event_id) DO
  NOTHING`.
- Si el `INSERT` no afecta filas (`rowcount == 0`), el evento ya se procesó y se omite.
- El marcado de dedup ocurre en la **misma transacción** que el `INSERT` de la notificación, de
  modo que ambos son atómicos: o se crean los dos, o ninguno.

El `event_id` se toma del envelope/payload (`event_id`, o `feedback_id` para feedback). Para
máxima garantía de idempotencia, el publicador (`sward_shared`) debe incluir siempre un
`event_id` estable en el envelope.

## Variables de entorno y secretos

Credenciales de BD (`username`/`password`) provienen de **AWS Secrets Manager**; nunca se
hardcodean.

| Variable | Descripción |
|---|---|
| `DATABASE_HOST` | Host de la BD de usuarios |
| `DATABASE_PORT` | Puerto (por defecto `5432`) |
| `DATABASE_NAME` | Nombre de la BD de usuarios |
| `DB_SECRET_ARN` | ARN del secret con `{username, password}` de usuarios |
| `CURSOS_DATABASE_HOST` | Host de la BD de cursos |
| `CURSOS_DATABASE_PORT` | Puerto de cursos (por defecto `5432`) |
| `CURSOS_DATABASE_NAME` | Nombre de la BD de cursos |
| `CURSOS_DB_SECRET_ARN` | ARN del secret de cursos |
| `AWS_REGION` | Región para Secrets Manager (por defecto `us-east-1`) |
| `LOG_LEVEL` | Nivel de log (por defecto `INFO`) |

Alternativa para entornos locales/de prueba: definir `DATABASE_URL` (y `CURSOS_DATABASE_URL`)
con la cadena de conexión completa, lo que omite Secrets Manager.

La Lambda necesita permisos IAM para `secretsmanager:GetSecretValue` sobre ambos secrets, además
de los permisos del event source mapping de SQS.

## Build y deploy

La imagen se construye sobre la base oficial de Lambda (`public.ecr.aws/lambda/python:3.11`) y
se publica vía GitHub Actions al hacer **push a la rama `deploy`**
(`.github/workflows/build-push.yml`, reutiliza el workflow compartido de la org `sward-UPC`).

```bash
# Push a la rama deploy dispara build + push de la imagen del contenedor
git push origin deploy
```

Build local del contenedor (opcional, para probar):

```bash
docker build -t sward-lambda-notificaciones .
```

El handler de entrada es `handler.lambda_handler` (definido en el `CMD` del `Dockerfile`).

## Testear

```bash
pip install -r requirements-dev.txt
pytest -q          # tests unitarios (no requieren BD ni psycopg2 instalado)
ruff check         # linter
```

Los tests unitarios cubren el parseo del payload y la resolución del `event_id`. La lógica de
BD usa imports diferidos de `psycopg2`, por lo que los tests corren sin el binario ni una BD
real; la cobertura de integración (inserts reales) se hace contra Postgres en un entorno con BD.

## Estructura

```
handler.py              # orquestación: parseo, despacho por detail-type, los 4 handlers
lib/
  db_client.py          # conexión a las 2 BDs (prefijo) + Secrets Manager
  idempotency.py        # dedup por event_id (processed_events, ON CONFLICT)
  logger.py             # logging JSON estructurado
tests/                  # tests unitarios (pytest)
Dockerfile              # imagen de contenedor para Lambda
.github/workflows/      # CI: build & push en push a `deploy`
```
