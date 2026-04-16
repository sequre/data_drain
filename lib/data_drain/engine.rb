# frozen_string_literal: true

require "duckdb"
require "pg"

module DataDrain
  # Motor principal de extracción y purga de datos (DataDrain).
  #
  # Orquesta el flujo ETL desde PostgreSQL hacia un Data Lake analítico
  # delegando la interacción del almacenamiento al adaptador configurado.
  class Engine
    include Observability
    include Observability::Timing
    # Inicializa una nueva instancia del motor de extracción.
    #
    # @param options [Hash] Diccionario de configuración para la extracción.
    # @option options [Time, DateTime, Date] :start_date Fecha y hora de inicio.
    # @option options [Time, DateTime, Date] :end_date Fecha y hora de fin.
    # @option options [String] :table_name Nombre de la tabla en PostgreSQL.
    # @option options [String] :folder_name (Opcional) Nombre de la carpeta destino.
    # @option options [String] :select_sql (Opcional) Sentencia SELECT personalizada.
    # @option options [Array<String, Symbol>] :partition_keys Columnas para particionar.
    # @option options [String] :primary_key (Opcional) Clave primaria para borrado. Por defecto 'id'.
    # @option options [String] :where_clause (Opcional) Condición SQL extra
    #   que filtra export, count e integrity check. Define "qué se archiva".
    # @option options [String] :purge_where_clause (Opcional) Condición SQL
    #   para el DELETE. Si se omite, usa :where_clause (backwards compatible).
    #   Pasar nil explícito para desactivar purga. Pasar '' (vacío) para purgar
    #   todo el rango de fechas sin filtro adicional (útil para archivar subset
    #   y borrar superset).
    #   Puede ser más amplia que :where_clause; filas que matchean
    #   :purge_where_clause pero no :where_clause se borran sin archivar ni
    #   verificar. Útil para limpieza de orphans/trash que no debe respaldarse.
    # @option options [Boolean] :skip_export (Opcional) Si true, no exporta
    #   a Parquet — solo valida y purga (para uso con GlueRunner).
    def initialize(options)
      @start_date = options.fetch(:start_date).beginning_of_day

      @end_date = options.fetch(:end_date).to_date.next_day.beginning_of_day

      @table_name = options.fetch(:table_name)
      Validations.validate_identifier!(:table_name, @table_name)

      @folder_name = options.fetch(:folder_name, @table_name)
      @select_sql = options.fetch(:select_sql, "*")
      @partition_keys = options.fetch(:partition_keys)
      @primary_key = options.fetch(:primary_key, "id")
      Validations.validate_identifier!(:primary_key, @primary_key)
      @where_clause = options[:where_clause]
      @purge_where_clause = options.fetch(:purge_where_clause, @where_clause)
      @bucket = options[:bucket]
      @skip_export = options.fetch(:skip_export, false)

      @config = DataDrain.configuration
      @config.validate_for_engine!
      @logger = @config.logger
      @adapter = DataDrain::Storage.adapter

      database = DuckDB::Database.open(":memory:")
      @duckdb = database.connect
    end

    # @return [Boolean] true si el flujo completó exitosamente, false si falló
    def call
      @durations = {}
      start_time = monotonic
      log_start

      setup_duckdb
      return skip_empty(start_time) if step_count.zero?

      if @skip_export
        safe_log(:info, "engine.skip_export", { table: @table_name })
      else
        step_export
      end
      return integrity_failed(start_time) unless step_verify

      step_purge
      log_complete(start_time)
      true
    end

    private

    # @api private
    def log_start
      safe_log(:info, "engine.start",
               { table: @table_name, start_date: @start_date.to_date, end_date: @end_date.to_date })
    end

    # @api private
    def step_count
      @pg_count = timed(:db_query) { get_postgres_count }
      @pg_count
    end

    # @api private
    def skip_empty(start_time)
      duration = monotonic - start_time
      safe_log(:info, "engine.skip_empty", {
                 table: @table_name,
                 duration_s: duration.round(2),
                 db_query_duration_s: @durations.fetch(:db_query, 0).round(2)
               })
      true
    end

    # @api private
    def step_export
      safe_log(:info, "engine.export_start", { table: @table_name, count: @pg_count })
      timed(:export) { export_to_parquet }
    end

    # @api private
    def step_verify
      timed(:integrity) { verify_integrity }
    end

    # @api private
    def step_purge
      timed(:purge) { purge_from_postgres }
    end

    # @api private
    def log_complete(start_time)
      duration = monotonic - start_time
      safe_log(:info, "engine.complete", {
                 table: @table_name,
                 duration_s: duration.round(2),
                 db_query_duration_s: @durations.fetch(:db_query, 0).round(2),
                 export_duration_s: @durations.fetch(:export, 0).round(2),
                 integrity_duration_s: @durations.fetch(:integrity, 0).round(2),
                 purge_duration_s: @durations.fetch(:purge, 0).round(2),
                 count: @pg_count
               })
    end

    # @api private
    def integrity_failed(start_time)
      duration = monotonic - start_time
      safe_log(:error, "engine.integrity_error", {
                 table: @table_name,
                 duration_s: duration.round(2),
                 count: @pg_count
               })
      false
    end

    # @api private
    # @return [String]
    def base_where_sql
      sql = date_range_sql
      sql += " AND #{@where_clause}" if @where_clause && !@where_clause.empty?
      sql
    end

    # @api private
    # @return [String]
    def purge_where_sql
      return nil if @purge_where_clause.nil?

      sql = date_range_sql
      sql += " AND #{@purge_where_clause}" unless @purge_where_clause.empty?
      sql
    end

    # @api private
    # @return [String]
    def date_range_sql
      "created_at >= '#{@start_date.to_fs(:db)}' AND created_at < '#{@end_date.to_fs(:db)}'"
    end

    # @api private
    def setup_duckdb
      @duckdb.query("INSTALL postgres; LOAD postgres;")
      @duckdb.query("SET max_memory='#{@config.limit_ram}';") if @config.limit_ram.present?
      @duckdb.query("SET temp_directory='#{@config.tmp_directory}'") if @config.tmp_directory.present?
      @duckdb.query("ATTACH '#{@config.duckdb_connection_string}' AS pg_source (TYPE POSTGRES, READ_ONLY)")

      # 💡 Magia del Adapter: Él sabe si cargar httpfs y setear credenciales o no hacer nada
      @adapter.setup_duckdb(@duckdb)
    end

    # @api private
    # @return [Integer]
    def get_postgres_count
      pg_sql = "SELECT COUNT(*) AS row_count FROM public.#{@table_name} WHERE #{base_where_sql}"
      pg_sql = pg_sql.gsub("'", "''")
      query = "SELECT row_count FROM postgres_query('pg_source', '#{pg_sql}')"
      @duckdb.query(query).first.first
    end

    # @api private
    def export_to_parquet
      # 💡 Magia del Adapter: Si es local crea las carpetas, si es S3 no hace nada.
      @adapter.prepare_export_path(@bucket, @folder_name)

      # Determinamos el path base de destino según el adaptador
      dest_path = if @config.storage_mode.to_sym == :s3
                    "s3://#{@bucket}/#{@folder_name}/"
                  else
                    File.join(@bucket,
                              @folder_name, "")
                  end

      pg_sql = "SELECT #{@select_sql} FROM public.#{@table_name} WHERE #{base_where_sql}"
      pg_sql = pg_sql.gsub("'", "''")

      query = <<~SQL
        COPY (
          SELECT #{@select_sql}
          FROM postgres_query('pg_source', '#{pg_sql}')
        ) TO '#{dest_path}'
        (
          FORMAT PARQUET,
          PARTITION_BY (#{@partition_keys.join(", ")}),
          COMPRESSION 'ZSTD',
          OVERWRITE_OR_IGNORE 1
        );
      SQL
      @duckdb.query(query)
    end

    # @api private
    # @return [Boolean]
    def verify_integrity
      # 💡 Magia del Adapter: Construye la ruta de búsqueda global ('**/*.parquet')
      archive_path = @adapter.build_path(@bucket, @folder_name, nil)

      begin
        query = <<~SQL
          SELECT COUNT(*)
          FROM read_parquet('#{archive_path}')
          WHERE #{base_where_sql}
        SQL
        parquet_result = @duckdb.query(query).first.first
      rescue DuckDB::Error => e
        safe_log(:error, "engine.parquet_read_error", { table: @table_name }.merge(exception_metadata(e)))
        return false
      end

      safe_log(:info, "engine.integrity_check",
               { table: @table_name, pg_count: @pg_count, parquet_count: parquet_result })
      @pg_count == parquet_result
    end

    # @api private
    def purge_from_postgres
      safe_log(:info, "engine.purge_start", { table: @table_name, batch_size: @config.batch_size })

      conn = PG.connect(
        host: @config.db_host,
        port: @config.db_port,
        user: @config.db_user,
        password: @config.db_pass,
        dbname: @config.db_name
      )

      unless @config.idle_in_transaction_session_timeout.nil?
        conn.exec("SET idle_in_transaction_session_timeout = #{@config.idle_in_transaction_session_timeout};")
      end

      total_deleted = purge_loop(conn)

      vacuum_if_needed(conn, total_deleted)
    ensure
      conn&.close
    end

    # @api private
    def vacuum_if_needed(conn, total_deleted)
      return unless @config.vacuum_after_purge
      return if total_deleted.zero?

      vacuum_start = monotonic
      dead_before = fetch_dead_tuple_count(conn)

      begin
        conn.exec("VACUUM ANALYZE #{@table_name};")
      rescue PG::Error => e
        safe_log(:warn, "engine.vacuum_error", {
          table: @table_name,
          dead_tuples_before: dead_before,
          rows_deleted_count: total_deleted,
          duration_s: (monotonic - vacuum_start).round(2)
        }.merge(exception_metadata(e)))
        return
      end

      dead_after = fetch_dead_tuple_count(conn)
      vacuum_duration = monotonic - vacuum_start

      safe_log(:info, "engine.vacuum_complete", {
                 table: @table_name,
                 duration_s: vacuum_duration.round(2),
                 dead_tuples_before: dead_before,
                 dead_tuples_after: dead_after,
                 rows_deleted_count: total_deleted
               })
    end

    # @api private
    def fetch_dead_tuple_count(conn)
      result = conn.exec_params(
        "SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = $1",
        [@table_name]
      )
      result.first&.dig("n_dead_tup")&.to_i || 0
    rescue PG::Error
      -1
    end

    # @api private
    # @param conn [PG::Connection]
    # @return [Integer] total de filas borradas
    def purge_loop(conn)
      delete_sql = build_delete_sql
      if delete_sql.nil?
        safe_log(:info, "engine.purge_skipped", { table: @table_name, reason: "no_purge_clause" })
        return 0
      end

      batches_processed = 0
      total_deleted = 0
      slow_batch_streak = 0

      loop do
        batch_start = monotonic
        result = conn.exec(delete_sql)
        batch_duration = monotonic - batch_start
        count = result.cmd_tuples
        break if count.zero?

        batches_processed += 1
        total_deleted += count

        slow_batch_streak = handle_batch_timing(batch_duration, count, slow_batch_streak)
        emit_heartbeat_if_due(batches_processed, total_deleted)

        sleep(@config.throttle_delay) if @config.throttle_delay.positive?
      end

      total_deleted
    end

    # @api private
    def handle_batch_timing(batch_duration, count, streak)
      if batch_duration > @config.slow_batch_threshold_s
        streak += 1
        safe_log(:warn, "engine.slow_batch", {
                   table: @table_name,
                   batch_duration_s: batch_duration.round(2),
                   batch_size: count,
                   streak: streak,
                   threshold_s: @config.slow_batch_threshold_s
                 })

        if streak == @config.slow_batch_alert_after
          safe_log(:warn, "engine.purge_degraded", {
                     table: @table_name,
                     consecutive_slow_batches: streak,
                     hint: "considerar índice composite o particionamiento (ver postgres-tuning.md)"
                   })
        end
        streak
      else
        0
      end
    end

    # @api private
    def emit_heartbeat_if_due(batches_processed, total_deleted)
      return unless (batches_processed % 100).zero?

      safe_log(:info, "engine.purge_heartbeat", {
                 table: @table_name,
                 batches_processed_count: batches_processed,
                 rows_deleted_count: total_deleted
               })
    end

    # @api private
    # @return [String, nil] SQL DELETE statement or nil if no purge clause
    def build_delete_sql
      where = purge_where_sql
      return nil if where.nil?

      <<~SQL
        DELETE FROM #{@table_name}
        WHERE #{@primary_key} IN (
          SELECT #{@primary_key} FROM #{@table_name}
          WHERE #{where}
          LIMIT #{@config.batch_size}
        )
      SQL
    end
  end
end
