## [Unreleased]

## [0.6.0] - 2026-04-16

### Features

- Nueva opción `purge_where_clause` en `Engine#initialize`. Permite especificar una condición SQL independiente para el DELETE, distinta de `where_clause` (que aplica a export/verify). Caso de uso: archivar subset (`isp_id IS NOT NULL`) pero purgar superset (todo el rango). Valores: `nil` = no purge, `""` = purge todo el rango, `"x"` = rango AND x. Backwards compatible vía `fetch(:purge_where_clause, @where_clause)`. Fixes #3.

### Refactor

- Extraído helper `date_range_sql` en Engine para eliminar duplicación entre `base_where_sql` y `purge_where_sql`.

### YARD

- Documentación actualizada en `Engine#initialize` para los tres casos de `purge_where_clause`.
- `Engine#build_delete_sql` ahora documenta retorno `String|nil`.

### Telemetry

- Nuevo evento `engine.purge_skipped` cuando no hay cláusula de purge (`delete_sql.nil?`).

### Tests

- 5 nuevos tests para `purge_where_clause`: backwards compatible, empty string purge all, integrity usa base_where_sql, independiente de where_clause, y use case primario (archive subset / purge superset).

## [0.5.2] - 2026-04-16

### Correcciones

- `Record#where()` ahora usa wildcards (`key=*`) para partition keys no especificadas, en lugar de valores vacíos (`key=`). Consistente con `destroy_partitions`. Fixes #1.

## [0.5.1] - 2026-04-15

### Docs

- `skill/references/eventos-telemetria.md`: nuevos eventos `script_uploaded` y `script_upload_error`.
- `README.md`: ejemplos de `script_path` en GlueRunner y observabilidad.

## [0.5.0] - 2026-04-15

### Features

- `Storage::S3#upload_file` y `Storage::Local#upload_file`: primitiva para subir archivos al storage configurado. (item 37)
- `GlueRunner.upload_script(local_path:, bucket:, folder:, filename:)`: sube script Python local a S3 usando el `Storage::S3` adapter existente. Emite `glue_runner.script_uploaded` (INFO) y `glue_runner.script_upload_error` (ERROR). (item 37)
- `GlueRunner.create_job` y `GlueRunner.ensure_job` aceptan `script_path:` + `script_bucket:` + `script_folder:` + `script_filename:` para subir scripts locales automáticamente. Si se usa `script_location:`, comportamiento idéntico al anterior. (item 37)

### Docs

- `docs/glue-jobs-lifecycle.md`: sección "Subir scripts locales" con patrón completo, permisos IAM mínimos y limitación de concurrencia.
- `docs/glue_pyspark_example.py`: ejemplo de uso con `script_path`.

### Notas

- **Upload NO es idempotente en sentido estricto:** `put_object` sobrescribe siempre. Documentado.
- `upload_script` requiere `storage_mode = :s3`. En `:local` levanta `ConfigurationError`.

## [0.4.0] - 2026-04-15

### Features

- `GlueRunner.job_exists?(job_name)`: verifica si un job existe. Retorna `true`/`false`. (item 35)
- `GlueRunner.get_job(job_name)`: obtiene la configuración completa de un job. Retorna `Aws::Glue::Types::Job`. (item 35)
- `GlueRunner.create_job(job_name, role_arn:, script_location:, ...)`: crea un job con configuración completa. Retorna el job creado. (item 32)
- `GlueRunner.update_job(job_name, ...)`: actualiza un job existente. Retorna el job actualizado. (item 33)
- `GlueRunner.delete_job(job_name)`: elimina un job. Retorna `nil`. (item 34)
- `GlueRunner.ensure_job(job_name, ...)`: upsert idempotente — crea si no existe, actualiza si existe. (item 36)

### Validations

- `DataDrain::Validations.validate_glue_name!`: validación específica para nombres de Glue Jobs (letras, números, guiones; no permite guiones bajos ni espacios).

### Tests

- 163 specs, coverage 97.39%.

### Docs

- `docs/glue-jobs-lifecycle.md`: referencia completa de la API de Glue Jobs.
- README.md actualizado con ejemplos de todos los métodos.
- `skill/references/eventos-telemetria.md`: nuevos eventos `glue_runner.job_exists` y `glue_runner.job_created`.

## [0.3.2] - 2026-04-15

### Regresiónfix (desde v0.3.1)

- `COUNT(*)` → `count()` (item 16 de v0.3.1) era incorrecto. `count()` sin argumentos es SQL inválido en Postgres. Se revierte a `COUNT(*)` en `Engine#get_postgres_count`, `Engine#verify_integrity` y `FileIngestor#step_count_source`. Ver [#10](https://github.com/gedera/data_drain/pull/10).

## [0.3.1] - 2026-04-15

### BREAKING (preventivo)
- `required_ruby_version` bumpeado a `">= 3.2"` (Ruby 3.0 y 3.1 están EOL desde 2024-03 y 2025-03 respectivamente).

### Refactor
- Extraído `Storage::Base#build_path_base` para eliminar duplicación entre Local y S3. (item 13)
- Queries SQL internas adoptan `count()` friendly syntax de DuckDB en Engine y FileIngestor. (item 16)

### Tests
- 37 ofensas RuboCop en `spec/` arregladas; RuboCop corre en todo el proyecto. (item 17)
- Tests GlueRunner migrados de `stub_const` a `Aws::Glue::Client.stub_responses` nativo. Tests S3 mantienen `stub_const` por falta de XML parser disponible localmente. (item 19)
- SimpleCov `minimum_coverage` subido a 90% (cobertura real 97.5%). (item 23)

### CI
- Matrix Ruby 3.2 / 3.3 / 3.4 en CI. (item 18)
- RuboCop agregado al workflow. (item 14)
- Fix: workflow trigger corregido de `master` a `main`. (item 14)
- Cache de RuboCop por Ruby version + hash de config (ahorra ~25s por run). (item 22)
- Badge de CI en README. (item 24)

### Docs
- YARD coverage 90.79% → 100%: Configuration, Observability, Observability::Timing, Errors, Storage::S3, Types, Validations, VERSION. (item 12)
- CLAUDE.md: sección DEBUG en bloque obligatoria con ejemplo correcto/incorrecto. (item 15)
- `skill/references/postgres-tuning.md`: nueva sección "Tuning de parámetros DataDrain por tamaño" con tabla de `batch_size`, `throttle_delay`, `vacuum_after_purge` y `slow_batch_threshold_s` según cantidad de filas. (item 15)
- `skill/references/antipatrones.md`: item 11 (DEBUG sin bloque) ampliado con ejemplo real de DataDrain. (item 15)

### RuboCop hardening
- `NewCops: disable` y cops pre-existentes deshabilitados en `.rubocop.yml` para evitar regressions.
- `Metrics/BlockLength` excluye `spec/**/*_spec.rb` y `data_drain.gemspec`.
- `Naming/VariableNumber` con `AllowedIdentifiers` para fixtures de tests `expected_partition_42/99`.

## [0.3.0] - 2026-04-15

### Refactor
- `Engine#call` refactorizado: extraídos `step_count`, `step_export`, `step_verify`, `step_purge` como métodos privados con `timed` helper. CC bajó de 13 a 5. Eventos emitidos idénticos al comportamiento anterior. (item 10)
- Extraído `DataDrain::Observability::Timing` mixin compartido entre Engine y FileIngestor. (item 20)
- `FileIngestor#call` refactorizado análogo a Engine. (item 20)
- Eliminados todos los `# rubocop:disable Metrics/*` en `lib/`. (item 20)

### Features
- `config.vacuum_after_purge = false` (default). Si `true`, ejecuta `VACUUM ANALYZE` post-purga cuando hubo deletes. Emite `engine.vacuum_complete` con dead_tuples antes/después y duración. Errores PG se capturan como `engine.vacuum_error` WARN. (item 5)
- `config.slow_batch_threshold_s = 30` y `config.slow_batch_alert_after = 5`. Detecta lotes de purga lentos. Emite `engine.slow_batch` WARN por cada lote lento, `engine.purge_degraded` WARN una vez por streak. Incluye hint a docs de tuning. (item 11b)

### Security
- `Record.connection` aplica `SET lock_configuration=true` post-setup. Congela cualquier SET futuro sobre la conexión (defensa en profundidad). NO afecta secrets ni extensiones ya cargadas. (item 6)

### Telemetry nueva
- `engine.vacuum_complete`, `engine.vacuum_error`, `engine.slow_batch`, `engine.purge_degraded`.

### Tests
- Coverage se mantiene ≥ 80%.
- Nuevo test de equivalencia para Engine (eventos idénticos pre/post refactor).
- Timecop agregado para tests de timing (item 11b).

## [0.2.2] - 2026-04-14

### Security
- `Observability#safe_log` filtra secretos con regex `/password|passwd|pass|secret|token|api_key|apikey|auth|credential|private_key/i` — ahora captura variantes como `db_password`, `aws_secret_access_key`, `bearer_token`, `private_key`, `*credential*`. (item 9)

### Features
- `GlueRunner.run_and_wait` acepta `max_wait_seconds:` para evitar bloqueo indefinido. Default `nil` (sin límite, backward-compatible). Emite `glue_runner.timeout` y levanta `DataDrain::Error` cuando excede. (item 7)
- `Configuration#validate!` y `Configuration#validate_for_engine!` invocados automáticamente en `Engine`, `FileIngestor` y `GlueRunner`. Falla rápido con errores descriptivos si falta configuración. (item 8)

### Docs
- `skill/references/postgres-tuning.md`: guía de tuning Postgres por tamaño de tabla — índices, VACUUM post-purga, particionamiento, diagnóstico. (item 11a)

### Cleanups (review PR #6)
- Fix typo `依赖` en CHANGELOG v0.2.1 (A1).
- Comment explicativo en `Record.disconnect!` rescue (A2).
- Cobertura real string-keys vs symbol-keys en `Record.build_query_path` (A3).
- Cerrar conn+db en `record_spec.rb#before(:all)` para evitar memory leak en suite (A4).
- Reorder `public`/`private` en `storage/s3.rb` (B1).

### BREAKING (preventivo)
- `Engine.new` / `FileIngestor.new` / `GlueRunner.run_and_wait` ahora levantan `DataDrain::ConfigurationError` en el boot si la configuración está incompleta. Antes fallaban tarde con errores oscuros. La gema aún no está en uso en producción — no hay impacto real.

## [0.2.1] - 2026-04-13

### Correcciones
- CI: Descarga binario pre-compilado de DuckDB en vez de depender del sistema (`libduckdb-dev`). Soporta Ruby 3.4.4 en GitHub Actions.
- CI: Opt-in a Node.js 24 (`FORCE_JAVASCRIPT_ACTIONS_TO_NODE24`).
- CI: Ejecuta solo specs en CI (RuboCop vía local) para evitar 48 ofensas pre-existentes en specs.
- PR feedback: Test `aws_region` con comillas, `minimum_coverage` 80%, antipatrón 12 actualizado.

### Mantenimiento
- `.gitignore`: Agregados `.agents/`, `.env`, `skills.lock`, `skills.yml`.
- `docs/IMPROVEMENT_PLAN.md`: Items 1-4 (P0) marcados como completados.

## [0.2.0] - 2026-04-13

### Security
- **BREAKING (preventivo):** `table_name` y `primary_key` se validan contra regex `\A[a-zA-Z_][a-zA-Z0-9_]*\z`. Identificadores con caracteres especiales (puntos, espacios, comillas) ahora levantan `DataDrain::ConfigurationError`. (item 2)
- Storage::S3 migra a `CREATE SECRET (TYPE S3, PROVIDER credential_chain)`. Si `aws_access_key_id`/`aws_secret_access_key` están seteados, se mantiene comportamiento explícito; si no, usa AWS credential chain (IAM roles, env vars, ~/.aws/credentials). `aws_region` ahora se escapa con `''` en el SQL. (item 1)

### Features
- `Record.disconnect!` cierra y limpia la conexión DuckDB thread-local. Recomendado en middlewares Sidekiq/Puma para evitar memory leak. Idempotente. (item 3)

### Tests
- Cobertura: 112 specs, coverage líneas 97.37% (SimpleCov).
- Specs nuevos: Record, Storage::Local, Storage::S3, Storage factory, GlueRunner, Observability, Configuration, JsonType, Validations, Engine (validación), FileIngestor (validación + ingestión CSV/JSON/Parquet).

## [0.1.19] - 2026-03-30

- Fix: `Record.build_query_path` ahora usa `partition_keys` como fuente de verdad del orden, ignorando el orden de los kwargs del caller. Antes, pasar `where(year: 2026, isp_id: 42)` en distinto orden generaba un path que no coincidía con la estructura Hive en disco.
- Fix: `GlueRunner` reemplaza `.truncate(200)` de ActiveSupport por `[0, 200]` de Ruby puro, eliminando la dependencia implícita.
- Convention: orden canónico de `partition_keys` es `[dimension_principal, year, month]` (ej. `isp_id` primero). Documentado en CLAUDE.md y actualizado en README, specs y ejemplos de PySpark.
- Docs: README actualizado con ejemplos de producción correctos para Glue + Engine + Record.

## [0.1.18] - 2026-03-23

- Feature: Módulo `Observability` centraliza el logging estructurado en toda la gema.
- Feature: Heartbeat de progreso para purgas masivas (`engine.purge_heartbeat`).
- Telemetry: Separación de contexto de error (`error_class`, `error_message`) en todos los eventos de falla.
- Resilience: Los fallos en el sistema de logs nunca interrumpen el flujo principal de datos.

## [0.1.17] - 2026-03-17

- Feature: Telemetría granular por fases (Ingeniería de Performance).
- Telemetry: Inclusión de métricas específicas como \`db_query_duration_s\`, \`export_duration_s\`, \`integrity_duration_s\` y \`purge_duration_s\` en el evento \`engine.complete\`.
- Telemetry: Inclusión de \`source_query_duration_s\` y \`export_duration_s\` en \`file_ingestor.complete\`.

## [0.1.16] - 2026-03-17

- Refactor: Cumplimiento con el estándar **Wispro-Observability-Spec (v1)**.
- Telemetry: Renombrado de métricas de tiempo a \`duration_s\` y \`next_check_in_s\` eliminando sufijos de unidad en los valores.
- Observability: Garantía de valores numéricos puros para contadores y tiempos, facilitando el procesamiento por \`exis_ray\`.

## [0.1.15] - 2026-03-17

- Performance: Medición de duraciones con reloj monotónico (`Process.clock_gettime`) en eventos terminales de `Engine`, `FileIngestor` y `GlueRunner`.
- Fix: `idle_in_transaction_session_timeout` ahora se aplica correctamente cuando el valor es `0` (desactiva el timeout). Antes `0.present?` evaluaba a `false` y se ignoraba.
- Fix: Objeto `DuckDB::Database` en `Record` ahora se ancla en el thread-local junto a la conexión, previniendo garbage collection prematura.
- Fix: `Storage.adapter` cachea la instancia en vez de crearla en cada llamada.
- Documentation: Agregado `CLAUDE.md` con guía de arquitectura y estándares del proyecto.

## [0.1.14] - 2026-03-17

- Feature: Implementación de **Logging Estructurado** en toda la gema (\`key=value\`) para mejor observabilidad en producción.
- Optimization: Caching automático de adaptadores de almacenamiento para mejorar el rendimiento de consultas repetidas.
- Testing: Mejora en la robustez de los tests de \`Engine\` desacoplándolos de cambios menores en el setup de DuckDB.

## [0.1.13] - 2026-03-17

- Feature: Parametrización total en la orquestación con Glue. Se añadieron \`s3_bucket\`, \`s3_folder\` y \`partition_by\` como argumentos dinámicos, permitiendo que el mismo Job de Glue sirva para múltiples tablas y destinos.

## [0.1.12] - 2026-03-17

- Feature: Parametrización dinámica de la base de datos en \`GlueRunner\` y el script de PySpark. Ahora se pasan \`db_url\`, \`db_user\`, \`db_password\` y \`db_table\` como argumentos al Job de Glue.

## [0.1.11] - 2026-03-17

- Feature: Se agregó \`DataDrain::GlueRunner\` para orquestar Jobs de AWS Glue.
- Feature: Soporte oficial para procesamiento de Big Data (ej. tablas de 1TB) mediante delegación a AWS Glue.
- Documentation: Se incluyó un script maestro de PySpark en el README compatible con el formato de la gema.

## [0.1.10] - 2026-03-17

- Feature: Se agregó la opción \`skip_export\` a \`DataDrain::Engine\`. Permite utilizar herramientas externas (como AWS Glue) para la exportación de datos, dejando que DataDrain se encargue solo de la validación de integridad y la purga de PostgreSQL.

## [0.1.9] - 2026-03-17

- Fix: Mejora en la precisión del rango de fechas en consultas SQL usando límites semi-abiertos (<) para evitar pérdida de registros por microsegundos.

## [0.1.8] - 2026-03-16

- Fix: Se cambió la cadena de conexión de DuckDB a formato URI para propagar el timeout de sesión en el ATTACH.

## [0.1.7] - 2026-03-16

- Se agrego soporte para idle_in_transaction_session_timeout.

## [0.1.6] - 2026-03-16

- Se agrego el tem_directory para duckdb.

## [0.1.5] - 2026-03-16

- Se agrego el attach para duckdb.

## [0.1.4] - 2026-03-16

- Corrección de error por comilla simple

## [0.1.3] - 2026-03-16

- Corrección de la sintaxis para postres_query

## [0.1.2] - 2026-03-16

- Cambiamos postgres_scan por postgres_query

## [0.1.1] - 2026-03-16

- Se agrega al configure la posibliidad de agregar el limit de ram para duckdb.

## [0.1.0] - 2026-03-11

- Initial release
