# DataDrain Expert

Skill de conocimiento completo sobre DataDrain. Consultame para cualquier pregunta sobre integración, arquitectura, API, errores y antipatrones.

## Glosario

- **DataDrain** — Micro-framework Ruby para ETL: extraer datos históricos de PostgreSQL → Parquet (S3/Local) → verificar integridad → purgar origen.
- **Engine** — Motor principal que orquesta el flujo Conteo → Export → Verify → Purge.
- **FileIngestor** — Convierte archivos crudos (CSV/JSON/Parquet) a Parquet particionado en el Data Lake.
- **Record** — Clase base ORM analítico (tipo ActiveRecord) read-only sobre Parquet vía DuckDB.
- **GlueRunner** — Orquestador de AWS Glue Jobs para tablas de gran volumen (>500GB-1TB). Soporta lifecycle completo: crear, actualizar, eliminar y verificar jobs.
- **Storage Adapter** — Patrón Strategy con dos implementaciones: `Storage::Local` y `Storage::S3`. Cacheado en `Storage.adapter`.
- **Observability** — Módulo mixín (`include`/`extend`) con `safe_log` resiliente y logging KV estructurado.
- **Hive Partitioning** — Estructura de carpetas `key1=val1/key2=val2/...` que DuckDB genera y consume nativamente para prefix scans eficientes.
- **Semi-abierto** — Convención de rangos `[start, end)` con `<` (no `<=`) para evitar pérdida de microsegundos en límites de fecha.
- **skip_export** — Modo del Engine donde delega export a herramienta externa (Glue/EMR) y solo verifica + purga.
- **purge_where_clause** — Condición SQL independiente para el DELETE. Permite archivar subset y purgar superset. nil = skip, "" = purge todo el rango, "x" = rango AND x.
- **ensure_job** — Wrapper idempotente de GlueRunner que crea o actualiza un job según config deseada. Incluye diffing de configuración para evitar API calls innecesarios.
- **changed_fields** — Helper privado de ensure_job que compara config deseada vs actual de un Glue Job y retorna qué campos difieren.
- **Heartbeat** — Log de progreso emitido cada 100 lotes en purgas masivas (tablas 1TB).
- **Wispro-Observability-Spec v1** — Estándar de logs KV: `component=` y `event=` primero, sufijo `_s` para tiempos float, `_count` para enteros, sin unidades en valores.

## Arquitectura

### Responsabilidad core

DataDrain resuelve el ciclo de vida de datos históricos en bases relacionales calientes: archivar a Data Lake con garantía matemática de integridad antes de purgar el origen.

### Componentes

```
┌──────────────┐    ┌──────────────┐    ┌──────────────┐
│  PostgreSQL  │───>│    Engine    │───>│  Data Lake   │
└──────────────┘    │  (DuckDB)    │    │ (S3 / Local) │
       ▲            └──────────────┘    └──────────────┘
       │                   │                   ▲
       │                   ▼                   │
       │            ┌──────────────┐           │
       └────purga───│  Verify OK?  │           │
                   └──────────────┘           │
                                              │
                          ┌──────────────┐    │
                          │ FileIngestor │────┘
                          └──────────────┘
                                              │
                          ┌──────────────┐    │
                          │   Record     │<───┘
                          │ (consultas)  │
                          └──────────────┘
```

### Flujo runtime de Engine

```
1. setup_duckdb     → ATTACH Postgres + setup adapter (httpfs si S3)
2. get_postgres_count → si 0, return true (skip)
3. export_to_parquet → COPY ... TO ... PARTITION_BY (...) ZSTD  [omitido si skip_export]
4. verify_integrity  → COUNT(*) Parquet == COUNT(*) Postgres
5. purge_from_postgres → DELETE en lotes throttled + heartbeat
```

### Decisiones de diseño

- **DuckDB en memoria** procesa millones de registros sin cargar objetos en RAM Ruby. Usa `ATTACH POSTGRES READ_ONLY` para leer origen y `COPY ... TO` para escribir Parquet.
- **Conexión DuckDB thread-local** en `Record`: cada thread inicializa una conexión persistente que se cachea en `Thread.current[:data_drain_duckdb] = { db:, conn: }`. El hash retiene la `Database` para evitar GC prematuro de la conexión.
- **Verify es la única puerta de seguridad** antes de purgar. Si retorna `false` (incluyendo `DuckDB::Error` al leer Parquet), la purga se aborta.
- **Storage Adapter cacheado**: `Storage.adapter` memoiza la instancia. Si se cambia `storage_mode` en runtime, llamar `Storage.reset_adapter!`.
- **Rangos semi-abiertos**: `created_at >= start AND created_at < end_boundary` donde `end_boundary = end_date.next_day.beginning_of_day`. Nunca `<= end_of_day`.

### Stack y dependencias

- Ruby `>= 3.2.0`
- Runtime: `activemodel >= 6.0`, `duckdb ~> 1.4`, `pg >= 1.2`, `aws-sdk-s3 ~> 1.114`, `aws-sdk-glue ~> 1.0`
- Versión actual: `0.6.0`

## API Pública (resumen)

### Configuración global

```ruby
DataDrain.configure do |config|
  config.storage_mode = :local | :s3
  config.aws_region, .aws_access_key_id, .aws_secret_access_key
  config.db_host, .db_port, .db_user, .db_pass, .db_name
  config.batch_size = 5000
  config.throttle_delay = 0.5
  config.idle_in_transaction_session_timeout = 0  # 0 = DESACTIVADO
  config.limit_ram = "2GB"
  config.tmp_directory = "/tmp/duckdb_work"
  config.logger = Rails.logger
end
```

### Operaciones principales

```ruby
# 1. ETL completo (Engine)
DataDrain::Engine.new(
  bucket:, start_date:, end_date:, table_name:,
  partition_keys: %w[isp_id year month],
  primary_key: "id",            # opcional
  where_clause: nil,             # opcional, SQL extra para export/verify
  purge_where_clause: nil,       # opcional, SQL para DELETE (nil=skip, ""=full range, "x"=range+x)
  skip_export: false,            # true delega export a Glue
  folder_name: nil,              # default = table_name
  select_sql: "*"                # default
).call  # => true (ok) | false (integrity fail)

# Purge subset vs archive superset (v0.6.0+)
DataDrain::Engine.new(
  bucket:, start_date:, end_date:, table_name:,
  partition_keys: %w[year month],
  where_clause: "isp_id IS NOT NULL",  # filtra qué se archiva
  purge_where_clause: ""               # purge TODO el rango (vacío = sin filtro adicional)
).call
# Resultado: export/verify sobre isp_id NOT NULL, purge sobre todo el rango

# 2. Ingesta de archivos crudos
DataDrain::FileIngestor.new(
  bucket:, source_path:, folder_name:,
  partition_keys: [],            # opcional
  select_sql: "*",               # opcional
  delete_after_upload: true      # opcional
).call

# 3. ORM analítico
class ArchivedX < DataDrain::Record
  self.bucket = "..."
  self.folder_name = "..."
  self.partition_keys = [:isp_id, :year, :month]  # ORDEN = jerarquía Hive
  attribute :id, :string
end
ArchivedX.where(limit: 10, isp_id: 42, year: 2026, month: 3)  # => Array
ArchivedX.find("uuid", isp_id: 42, year: 2026, month: 3)       # => instance | nil
ArchivedX.destroy_all(isp_id: 42)                               # => Integer (particiones borradas)

# 4. Glue para tablas 1TB+
DataDrain::GlueRunner.run_and_wait("job-name", { "--key" => "val" }, polling_interval: 30)

# 4b. Glue Jobs Lifecycle (v0.4.0+)
# Verificar si existe
DataDrain::GlueRunner.job_exists?("my-job")  # => true/false

# Obtener config completa
job = DataDrain::GlueRunner.get_job("my-job")  # => Aws::Glue::Types::Job

# Crear job con script local (v0.5.0+)
job = DataDrain::GlueRunner.create_job(
  "my-job",
  role_arn: "arn:aws:iam::123:role/GlueRole",
  script_path: "scripts/glue/export.py",  # local → S3 automático
  script_bucket: "my-bucket",
  script_folder: "scripts",
  timeout: 1440,
  max_retries: 2
)

# Upsert idempotente con diffing de config
job = DataDrain::GlueRunner.ensure_job(
  "my-job",
  role_arn: "arn:aws:iam::123:role/GlueRole",
  script_path: "scripts/glue/export.py",
  script_bucket: "my-bucket"
)

# Eliminar job (idempotente)
DataDrain::GlueRunner.delete_job("my-job")  # => true/false
```

Detalle completo de firmas, parámetros, retornos y comportamientos en [API Detallada](references/api-detallada.md).

## FAQ

### ¿Cuándo usar `Engine` directo vs `GlueRunner` + `Engine(skip_export: true)`?

`Engine` directo soporta hasta ~10-50GB cómodamente. Para tablas >500GB-1TB delegar el export a AWS Glue (Apache Spark distribuido) y usar `Engine(skip_export: true)` solo para verificar integridad y purgar Postgres. DataDrain en este modo solo lee Parquet (no exporta) y borra origen una vez confirmados los conteos.

### ¿Qué pasa si `verify_integrity` falla?

`Engine#call` retorna `false` y **no ejecuta la purga**. Emite log `engine.integrity_error`. Si la falla viene de no poder leer el Parquet (`DuckDB::Error`), emite `engine.parquet_read_error` y también retorna `false`. Es la única salvaguarda matemática del sistema.

### ¿Cómo cambiar `storage_mode` en runtime?

```ruby
DataDrain.configure { |c| c.storage_mode = :s3 }
DataDrain::Storage.reset_adapter!  # OBLIGATORIO, sino se sigue usando el adapter cacheado
```

### ¿Por qué `idle_in_transaction_session_timeout = 0`?

`0` **desactiva** el timeout (sin límite de tiempo). Es mandatorio para purgas de gran volumen donde un lote puede tardar segundos. Internamente se valida con `!nil?` (no `.present?`) porque `0.present?` es `false` en Rails.

### ¿El orden de `partition_keys` importa?

Sí, **crítico**. Determina la jerarquía Hive en disco. El orden al **escribir** (Engine/FileIngestor) debe ser idéntico al declarado en el modelo `Record` que lee. Mismatch → DuckDB retorna vacío sin error. Convención canónica: `[dimension_principal, year, month]` (mayor cardinalidad o filtro más usado primero).

### ¿La conexión DuckDB es thread-safe?

Sí. `Record.connection` mantiene una conexión por thread vía `Thread.current`. En Puma/Sidekiq cada worker thread tiene la suya. La conexión nunca se cierra explícitamente (persiste mientras vive el thread). `Engine` y `FileIngestor` crean su propia conexión efímera por instancia y la cierran en `ensure`.

### ¿DataDrain valida los nombres de tabla?

No. `table_name`, `select_sql` y `where_clause` se interpolan directamente en SQL. La gema asume que estos valores vienen de código de aplicación (no de input de usuario). En `Record.find` el `id` sí se sanitiza (escape de comillas simples).

### ¿Cómo evito OOM con tablas grandes?

Setear `limit_ram` (ej. `"2GB"`) y `tmp_directory` (en SSD). DuckDB hará spill-to-disk automáticamente. Para tablas >500GB delegar a Glue.

### ¿Los logs incluyen `source=`?

No. La gema NO emite `source=` manualmente — lo inyecta automáticamente `exis_ray` (logger middleware externo) cuando está presente. Si no usás `exis_ray`, agregalo con un wrapper de logger.

### ¿Qué formato tienen los logs?

`component=data_drain event=<clase>.<suceso> [campos KV]`. Tiempos con sufijo `_s` y valor float. Contadores con `_count` y valor integer. Sin unidades en los valores. Detalle en [Eventos y Telemetría](references/eventos-telemetria.md).

### ¿Cómo subo un script Glue desde mi repo?

Desde v0.5.0 podés usar `script_path:` en lugar de `script_location:`:

```ruby
DataDrain::GlueRunner.ensure_job(
  "my-export-job",
  script_path: "scripts/glue/export.py",
  script_bucket: "my-bucket",
  script_folder: "scripts",
  role_arn: ENV["GLUE_ROLE_ARN"]
)
```

La gema sube el script a S3 usando el `Storage::S3` adapter existente
(con `credential_chain` si tenés IAM role). **Requiere `storage_mode = :s3`**.
Si `storage_mode = :local`, levanta `ConfigurationError`.

**Overwrite:** cada invocación sobrescribe el archivo en S3. Útil para que
el script siga al código del repo. Si necesitás versionar, usar `script_filename:`
con hash o timestamp.

## Errores

Catálogo top. Detalle completo y resolución en [API Detallada](references/api-detallada.md).

### `DataDrain::Error`
Clase base. Toda excepción del framework hereda de acá.

### `DataDrain::ConfigurationError`
Levantado cuando falta configuración obligatoria. **Causa típica:** olvidar `aws_*` con `storage_mode = :s3`. **Resolución:** completar el bloque `DataDrain.configure`.

### `DataDrain::IntegrityError`
Reservado para fallos matemáticos en verificación. Actualmente `Engine#call` retorna `false` en lugar de levantarlo. **Resolución:** investigar mismatch entre conteo Postgres y conteo Parquet.

### `DataDrain::StorageError`
Problemas interactuando con disco local, S3 o DuckDB. **Causa típica:** credenciales AWS inválidas, bucket inexistente, permisos S3 insuficientes.

### `DataDrain::Storage::InvalidAdapterError`
`storage_mode` no reconocido. **Causa:** valor distinto de `:local` o `:s3`. **Resolución:** corregir configuración.

### `DuckDB::Error` (no envuelto)
Errores de query DuckDB. En `Engine#verify_integrity` se captura y se loguea como `engine.parquet_read_error` retornando `false`. En `FileIngestor#call` se captura y se loguea como `file_ingestor.duckdb_error` retornando `false`. En `Record` se captura en `execute_and_instantiate` y retorna `[]`.

### `RuntimeError` desde `GlueRunner`
Levantado cuando un Job de Glue termina con estado `FAILED`, `STOPPED` o `TIMEOUT`. **Mensaje:** `"Glue Job <name> (Run ID: <id>) falló con estado <status>."`

### `Aws::Glue::Errors::EntityNotFoundException`
Job de Glue no existe. En `job_exists?` se rescata y retorna `false`. En `get_job`, `update_job` y `delete_job` se propaga.

### `Aws::Glue::Errors::ServiceError`
Error genérico de AWS Glue. Se propaga en todos los métodos de lifecycle. Los métodos emiten `glue_runner.job_*_error` antes de propagar.

## Antipatrones

Catálogo completo en [Antipatrones](references/antipatrones.md). Resumen de los más críticos:

1. **Bypassear `verify_integrity`** llamando `purge_from_postgres` directo — rompe la única garantía de seguridad.
2. **Mismatch en orden de `partition_keys`** entre escritura y lectura — DuckDB devuelve vacío sin error.
3. **`storage_mode` cambiado sin `reset_adapter!`** — sigue usando el adapter viejo cacheado.
4. **Validar `idle_in_transaction_session_timeout` con `.present?`** — `0.present?` es `false`, ignora la config.
5. **Usar `<= end_of_day`** en rangos de fecha — pierde registros con microsegundos.
6. **Loguear `source=`** manualmente — duplica el campo que inyecta `exis_ray`.
7. **Usar nombres de Glue Job con guiones bajos al inicio** — `validate_glue_name!` rechaza `_my-job`. Usar `my-job` o `my_job` (sin underscore inicial).

## Referencias

- [API Detallada](references/api-detallada.md) — Firmas completas, parámetros, retornos y comportamientos de cada clase pública.
- [Glue Jobs Lifecycle](https://github.com/sequre/data_drain/blob/main/docs/glue-jobs-lifecycle.md) — Guía completa de gestión de AWS Glue Jobs: crear, actualizar, eliminar, verificar y ejecutar jobs idempotentemente.
- [Eventos y Telemetría](references/eventos-telemetria.md) — Catálogo completo de eventos KV emitidos por la gema.
- [Antipatrones](references/antipatrones.md) — Qué NO hacer y alternativas correctas.
- [Postgres Tuning](references/postgres-tuning.md) — Índices, VACUUM, particionamiento y diagnóstico por tamaño de tabla.
