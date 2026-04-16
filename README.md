# DataDrain

[![CI](https://github.com/sequre/data_drain/actions/workflows/main.yml/badge.svg)](https://github.com/sequre/data_drain/actions/workflows/main.yml)

Micro-framework Ruby para extraer, archivar y purgar datos históricos de PostgreSQL hacia un Data Lake (S3 o disco local) en formato Parquet, usando DuckDB en memoria.

## Características

- **ETL de alto rendimiento:** millones de registros de Postgres a Parquet sin cargar objetos en RAM Ruby.
- **File ingestion:** convierte CSV, JSON o Parquet locales a Parquet (ZSTD) particionado y los sube a S3.
- **Hive partitioning:** organiza archivos en `key=val/key=val/...` para prefix scans eficientes.
- **Storage adapters:** soporte transparente para disco local y AWS S3.
- **Integridad garantizada:** verificación matemática Postgres vs Parquet antes de cualquier `DELETE`.
- **ORM analítico:** clase base `DataDrain::Record` (compatible `ActiveModel`) para consultar y purgar particiones históricas.
- **Observabilidad estructurada:** logs `key=value` compatibles con Datadog, CloudWatch y `exis_ray`. Fallos del logger nunca interrumpen el flujo principal.

## Instalación

```ruby
# Gemfile
gem 'data_drain', git: 'https://github.com/sequre/data_drain.git', branch: 'main'
```

```bash
bundle install
```

## Configuración

```ruby
# config/initializers/data_drain.rb
DataDrain.configure do |config|
  config.storage_mode = ENV.fetch('STORAGE_MODE', 'local').to_sym  # :local o :s3

  # AWS S3 (solo si storage_mode == :s3)
  config.aws_region            = ENV['AWS_REGION']
  config.aws_access_key_id     = ENV['AWS_ACCESS_KEY_ID']
  config.aws_secret_access_key = ENV['AWS_SECRET_ACCESS_KEY']

  # PostgreSQL origen (solo para Engine)
  config.db_host = ENV.fetch('DB_HOST', '127.0.0.1')
  config.db_port = ENV.fetch('DB_PORT', '5432')
  config.db_user = ENV.fetch('DB_USER', 'postgres')
  config.db_pass = ENV.fetch('DB_PASS', '')
  config.db_name = ENV.fetch('DB_NAME', 'core_production')

  # Tuning de purga
  config.batch_size                          = 5000  # registros por DELETE
  config.throttle_delay                      = 0.5   # segundos entre lotes
  config.idle_in_transaction_session_timeout = 0     # 0 = DESACTIVADO (mandatorio en purgas masivas)

  # Tuning de DuckDB
  config.limit_ram     = '2GB'                # evita OOM en contenedores
  config.tmp_directory = '/tmp/duckdb_work'   # spill-to-disk (preferir SSD/NVMe)

  # Tuning de Postgres: ver skill/references/postgres-tuning.md
  # Índices para DELETE, VACUUM post-purga, particionamiento para tablas >100GB

  config.logger = Rails.logger
end
```

## Uso

### Ingesta de archivos crudos (FileIngestor)

```ruby
DataDrain::FileIngestor.new(
  bucket:              'my-bucket-store',
  source_path:         '/tmp/netflow_metrics.csv',
  folder_name:         'netflow',
  partition_keys:      %w[isp_id year month],
  select_sql:          "*, EXTRACT(YEAR FROM timestamp) AS year, EXTRACT(MONTH FROM timestamp) AS month",
  delete_after_upload: true
).call
```

### Extracción y purga (Engine)

Ventanas rodantes de retención: archivar 6 meses atrás y purgar el origen.

```ruby
DataDrain::Engine.new(
  bucket:         'my-bucket-store',
  start_date:     6.months.ago.beginning_of_month,
  end_date:       6.months.ago.end_of_month,
  table_name:     'versions',
  partition_keys: %w[year month]
).call
```

### Modo `skip_export` (delegar export a Glue/EMR)

DataDrain solo verifica integridad y purga; el export ya lo hizo otra herramienta.

```ruby
DataDrain::Engine.new(
  bucket:         'my-bucket-store',
  start_date:     6.months.ago.beginning_of_month,
  end_date:       6.months.ago.end_of_month,
  table_name:     'versions',
  partition_keys: %w[year month],
  skip_export:    true
).call
```

### Purge subset vs archive superset

Caso común: archivar filas válidas (`isp_id IS NOT NULL`) pero borrar superset (válidas + trash).

```ruby
# Archiva solo isp_id NOT NULL, verifica integridad solo sobre esos,
# pero purga TODO el mes (NULL + NOT NULL) con batching/throttling/vacuum
DataDrain::Engine.new(
  bucket:             'my-bucket-store',
  start_date:         6.months.ago.beginning_of_month,
  end_date:           6.months.ago.end_of_month,
  table_name:         'versions',
  partition_keys:     %w[year month],
  where_clause:       'isp_id IS NOT NULL',  # filtra qué se archiva
  purge_where_clause: ''                       # purge TODO el mes (vacío = sin filtro adicional)
).call
```

**Resultado:** Export/verify cuentan y comparan solo `isp_id NOT NULL`. Purge borra el mes completo con batching, throttling y vacuum del `purge_loop`.

### Orquestación con AWS Glue (tablas 1TB+)

```ruby
# Verificar si un job existe
DataDrain::GlueRunner.job_exists?("my-glue-export-job")
# => true / false

# Obtener configuración de un job
job = DataDrain::GlueRunner.get_job("my-glue-export-job")
# => Aws::Glue::Types::Job (Name, Command, DefaultArguments, etc.)

# Crear un job con script local (v0.5.0+)
job = DataDrain::GlueRunner.create_job(
  "my-glue-export-job",
  role_arn: "arn:aws:iam::123:role/GlueServiceRole",
  script_path: "scripts/glue/export.py",  # local → S3 automático
  script_bucket: "my-bucket",
  script_folder: "scripts",
  default_arguments: { "--extra-files" => "s3://my-bucket/scripts/udf.py" },
  timeout: 1440,
  max_retries: 2
)

# Asegurar job idempotente (crea si no existe, actualiza si existe)
job = DataDrain::GlueRunner.ensure_job(
  "my-glue-export-job",
  role_arn: "arn:aws:iam::123:role/GlueServiceRole",
  script_path: "scripts/glue/export.py",
  script_bucket: "my-bucket",
  script_folder: "scripts"
)

# Eliminar un job
DataDrain::GlueRunner.delete_job("my-glue-export-job")

# Ejecutar y esperar
DataDrain::GlueRunner.run_and_wait(
  "my-glue-export-job",
  {
    "--start_date"   => start_date.to_fs(:db),
    "--end_date"     => end_date.to_fs(:db),
    "--s3_bucket"    => bucket,
    "--s3_folder"    => table,
    "--db_url"       => "jdbc:postgresql://#{config.db_host}:#{config.db_port}/#{config.db_name}",
    "--db_user"      => config.db_user,
    "--db_password"  => config.db_pass,
    "--db_table"     => table,
    "--partition_by" => "isp_id,year,month"
  }
)

DataDrain::Engine.new(
  bucket:, folder_name: table, start_date:, end_date:,
  table_name: table, partition_keys: %w[isp_id year month],
  skip_export: true
).call
```

### Consultar el Data Lake (Record)

```ruby
class ArchivedVersion < DataDrain::Record
  self.bucket         = 'my-bucket-storage'
  self.folder_name    = 'versions'
  self.partition_keys = [:isp_id, :year, :month]  # orden = jerarquía Hive

  attribute :id,             :string
  attribute :item_type,      :string
  attribute :event,          :string
  attribute :created_at,     :datetime
  attribute :object,         :json
  attribute :object_changes, :json
end

# Búsqueda puntual aislando la partición exacta
ArchivedVersion.find("uuid", isp_id: 42, year: 2026, month: 3)

# Colecciones
ArchivedVersion.where(limit: 10, isp_id: 42, year: 2026, month: 3)

# Eliminación (retención y cumplimiento)
ArchivedVersion.destroy_all(isp_id: 42)              # todo el historial de un cliente
ArchivedVersion.destroy_all(year: 2024, month: 3)    # un mes globalmente
```

## Convenciones críticas

- **Rangos de fecha semi-abiertos:** siempre `created_at >= START AND created_at < END_BOUNDARY`. Nunca `<= end_of_day`.
- **Orden de `partition_keys`:** debe coincidir entre escritura (Engine/FileIngestor) y lectura (Record). Mismatch → DuckDB devuelve vacío sin error.
- **Cambiar `storage_mode` en runtime:** llamar `DataDrain::Storage.reset_adapter!` después.
- **`verify_integrity`** es la única salvaguarda antes de purgar. Si falla, el flujo retorna `false` y aborta el `DELETE`.

## Observabilidad

```
component=data_drain event=engine.complete table=versions duration_s=12.4 export_duration_s=8.1 purge_duration_s=3.9 count=150000
component=data_drain event=engine.purge_heartbeat table=versions batches_processed_count=100 rows_deleted_count=500000
component=data_drain event=glue_runner.script_uploaded local_path=scripts/glue/export.py s3_path=s3://my-bucket/scripts/export.py bytes=4521
component=data_drain event=glue_runner.failed job=my-export-job run_id=jr_abc123 status=FAILED duration_s=301.0
```

Formato `key=value`. Tiempos con sufijo `_s` (Float). Contadores con `_count` (Integer). Sin unidades en valores. Fallos internos del logger nunca interrumpen el flujo principal.

## Contribuir

```bash
bundle install
bundle exec rspec       # tests
bundle exec rubocop     # linting
bin/console             # REPL
```

## Licencia

MIT.
