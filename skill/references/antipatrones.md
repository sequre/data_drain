# Antipatrones

Qué NO hacer en DataDrain. Cada antipatrón incluye código incorrecto, razón y alternativa correcta.

## 1. Bypassear `verify_integrity` para purgar más rápido

**Incorrecto:**
```ruby
engine = DataDrain::Engine.new(...)
engine.send(:setup_duckdb)
engine.send(:purge_from_postgres)  # SIN verificar antes
```

**Razón:** `verify_integrity` es la **única salvaguarda matemática** entre la exportación y el `DELETE` definitivo. Si se omite, podés borrar datos que no fueron archivados (corrupción silenciosa, archivo Parquet vacío, mismatch de fechas, etc.).

**Alternativa:** Siempre usar `Engine#call`. Si necesitás solo verificar+purgar (porque el export lo hizo Glue/EMR), usar `skip_export: true` — el verify sigue siendo obligatorio dentro del flujo.

---

## 2. Mismatch en orden de `partition_keys` entre escritura y lectura

**Incorrecto:**
```ruby
# Engine escribe con orden A
Engine.new(partition_keys: %w[year month isp_id], ...).call

# Record lee con orden B
class ArchivedX < DataDrain::Record
  self.partition_keys = [:isp_id, :year, :month]  # MISMATCH
end
```

**Razón:** El orden de `partition_keys` determina la jerarquía Hive en disco (`year=X/month=Y/isp_id=Z`). Si Record lee con otro orden, el path generado no coincide y **DuckDB devuelve `[]` sin error**. La falla es silenciosa.

**Alternativa:** Mantener orden idéntico en escritura (Engine/FileIngestor) y lectura (Record). Convención canónica: `[dimension_principal, year, month]` (mayor cardinalidad o filtro más usado primero).

---

## 3. Cambiar `storage_mode` sin resetear el adapter

**Incorrecto:**
```ruby
DataDrain.configure { |c| c.storage_mode = :s3 }
DataDrain::Engine.new(...).call  # Sigue usando Local cacheado si ya se inicializó
```

**Razón:** `Storage.adapter` es memoizado (`@adapter ||= ...`). Cambiar `storage_mode` después de la primera invocación no tiene efecto.

**Alternativa:**
```ruby
DataDrain.configure { |c| c.storage_mode = :s3 }
DataDrain::Storage.reset_adapter!
```

---

## 4. Validar `idle_in_transaction_session_timeout` con `.present?`

**Incorrecto:**
```ruby
if @config.idle_in_transaction_session_timeout.present?  # 0.present? == false
  conn.exec("SET ... = #{...};")
end
```

**Razón:** El valor `0` significa **timeout desactivado** (sin límite), que es exactamente lo que querés en purgas masivas. `0.present?` es `false` en Rails, así que `0` se ignora silenciosamente y Postgres aplica el timeout default.

**Alternativa:** Usar `!nil?`:
```ruby
unless @config.idle_in_transaction_session_timeout.nil?
  conn.exec("SET ... = #{@config.idle_in_transaction_session_timeout};")
end
```

---

## 5. Usar `<= end_of_day` en rangos de fecha

**Incorrecto:**
```ruby
"created_at >= '#{start.beginning_of_day}' AND created_at <= '#{end_date.end_of_day}'"
```

**Razón:** `end_of_day` retorna `23:59:59.999999`. Registros con timestamps en los microsegundos siguientes (`23:59:59.9999995`) quedan fuera o cruzados según floor/ceil del cliente. Con `BETWEEN` o `<=` la pérdida de filas es silenciosa.

**Alternativa:** Rango semi-abierto con `<` y boundary del próximo periodo:
```ruby
"created_at >= '#{start.beginning_of_day}' AND created_at < '#{end_date.next_day.beginning_of_day}'"
```

---

## 6. Loguear `source=` manualmente

**Incorrecto:**
```ruby
safe_log(:info, "engine.start", { source: "data_drain", table: @table_name })
```

**Razón:** El campo `source=` lo inyecta automáticamente el middleware `exis_ray` (identifica el entrypoint: `http`, `sidekiq`, `task`, `system`). Emitirlo manualmente lo duplica o lo sobrescribe con un valor incorrecto.

**Alternativa:** Nunca incluir `source` en metadata. Solo `component` (automático vía `observability_name`) + `event` + campos de negocio.

---

## 7. Olvidar `private_class_method` al usar `extend Observability`

**Incorrecto:**
```ruby
class GlueRunner
  extend Observability
  # safe_log queda público — cualquiera puede llamar GlueRunner.safe_log(...)
end
```

**Razón:** `extend` hace los métodos del módulo accesibles como métodos de clase **públicos**. Eso filtra una API interna y rompe encapsulación.

**Alternativa:**
```ruby
class GlueRunner
  extend Observability
  private_class_method :safe_log, :exception_metadata, :observability_name
end
```

---

## 8. Olvidar `include Observability` en clases de instancia

**Incorrecto:**
```ruby
class Engine
  # falta include
  def call
    safe_log(:info, "engine.start", {})  # NoMethodError
  end
end
```

**Razón:** Sin `include`, `safe_log` no existe en la clase. Falla en runtime al primer evento.

**Alternativa:**
```ruby
class Engine
  include Observability
  def call
    safe_log(:info, "engine.start", {})
  end
end
```

---

## 9. Agregar lógica de infraestructura en `Observability`

**Incorrecto:** Agregar al módulo `Observability` métodos como `current_memory_mb` que usen backticks (`` `ps` ``) o `Process` para inferir métricas del sistema.

**Razón:** `Observability` es un **módulo de logging genérico**, reusable en otras gemas. Mezclarle lógica de infraestructura lo acopla al runtime específico y rompe portabilidad.

**Alternativa:** Métricas de infraestructura van en otro módulo (ej. `Telemetry::Process`) o en el caller. `Observability` solo formatea y emite logs.

---

## 10. Usar `Time.now` para medir duraciones

**Incorrecto:**
```ruby
start = Time.now
do_work
duration = Time.now - start
```

**Razón:** `Time.now` es wall clock — cambia con NTP, cambios de zona horaria, leap seconds. Mide tiempos negativos o saltos. No es apto para latencia.

**Alternativa:**
```ruby
start = Process.clock_gettime(Process::CLOCK_MONOTONIC)
do_work
duration = Process.clock_gettime(Process::CLOCK_MONOTONIC) - start
```

---

## 11. Loguear DEBUG sin bloque

**Incorrecto:**
```ruby
logger.debug("query=#{expensive_serialize(obj)}")  # Siempre evalúa, incluso si DEBUG off
```

**Razón:** Sin bloque, el string se construye siempre, incluso cuando el nivel DEBUG está desactivado en producción. Costo invisible.

**Alternativa:**
```ruby
logger.debug { "query=#{expensive_serialize(obj)}" }
```

**Ejemplo real en DataDrain:**

```ruby
# Incorrecto — el query puede tener MB de datos en partition_by, se serializa siempre:
logger.debug("export_query=#{query}")

# Correcto — solo se serializa si DEBUG está activo:
logger.debug { "export_query=#{query}" }
```

---

## 12. Asumir que `Record.connection` se puede cerrar manualmente

**Incorrecto:**
```ruby
ArchivedX.where(...)
ArchivedX.connection.close  # Rompe la siguiente query del mismo thread
```

**Razón:** `Record.connection` es thread-local y persistente — diseñada para amortizar el costo de cargar `httpfs` y credenciales. Cerrarla obliga a reconectar todo en la próxima query y puede dejar el `Thread.current` apuntando a una conexión muerta (`Database` GC'd).

**Alternativa:** No usar `Record.connection.close` directamente. Si necesitás cerrar (Sidekiq/Puma middleware), usar `Record.disconnect!` que cierra `db` + `conn` y limpia `Thread.current` atómicamente. En threads de larga vida, esto previene memory leak.

---

## 13. Pasar input de usuario a `select_sql` o `where_clause`

**Incorrecto:**
```ruby
DataDrain::Engine.new(
  table_name: params[:table],   # input usuario interpolado en SQL
  where_clause: params[:filter],
  ...
).call
```

**Razón:** `select_sql` y `where_clause` se interpolan **directamente en SQL** (no son prepared statements). Input de usuario abre vector de SQL injection.

**Nota:** `table_name` y `primary_key` (en `Engine#initialize`) y `folder_name` (en `FileIngestor#initialize`) se validan con regex `\A[a-zA-Z_][a-zA-Z0-9_]*\z`. Si el valor no matchea, levantan `DataDrain::ConfigurationError`. `select_sql` y `where_clause` siguen siendo trusted (no se validan).

**Alternativa:** `table_name` y `primary_key` ahora están protegidos contra injection trivial. `select_sql` y `where_clause` deben venir de código de aplicación (constantes, configuración, jobs con valores fijos).

---

## 14. Confiar en que `GlueRunner` tiene timeout máximo

**Incorrecto:**
```ruby
DataDrain::GlueRunner.run_and_wait("job", args)  # Sin timeout, puede bloquearse
```

**Razón:** Si Glue queda colgado en `RUNNING`, bloquea indefinidamente.

**Alternativa:** Usar `max_wait_seconds:` (desde v0.2.2):
```ruby
GlueRunner.run_and_wait("job", args, max_wait_seconds: 3600)  # 1h max
```

---

## 15. Llamar `Engine.new` con configuración incompleta

**Incorrecto:**
```ruby
DataDrain::Engine.new(table_name: "versions", start_date: ..., end_date: ...)
# donde DataDrain.configuration no tiene db_name seteado
```

**Razón:** Desde v0.2.2, `Engine#initialize` llama `config.validate_for_engine!` que verifica `db_host`, `db_user`, `db_name`. Si alguno falta, levanta `DataDrain::ConfigurationError`.

**Alternativa:** Asegurar que `db_name`, `db_user` y `db_host` estén seteados en `DataDrain.configure` antes de llamar `Engine.new`. Si se usa auth peer/trust, `db_pass` puede ser `nil`.
