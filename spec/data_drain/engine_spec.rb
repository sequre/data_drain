# frozen_string_literal: true

RSpec.describe DataDrain::Engine do
  let(:bucket) { "tmp/test_lake" }
  let(:base_options) do
    {
      bucket: bucket,
      start_date: Time.new(2026, 3, 1),
      end_date: Time.new(2026, 3, 31),
      partition_keys: %w[year month]
    }
  end

  let(:engine) { described_class.new(base_options.merge(table_name: "versions")) }

  let(:mock_duckdb) { instance_double(DuckDB::Connection) }
  let(:mock_pg_conn) { instance_double(PG::Connection) }
  let(:mock_pg_result) { instance_double(PG::Result) }

  before do
    DataDrain.configure do |c|
      c.db_name = "test_db"
      c.db_user = "test_user"
    end
    allow_any_instance_of(DuckDB::Database).to receive(:connect).and_return(mock_duckdb)
    allow(PG).to receive(:connect).and_return(mock_pg_conn)
    allow(mock_pg_conn).to receive(:close)
  end

  describe "validación de identificadores" do
    it "rechaza table_name con punto y coma (SQL injection attempt)" do
      expect do
        described_class.new(base_options.merge(table_name: "x; DROP TABLE y"))
      end.to raise_error(DataDrain::ConfigurationError, /table_name/)
    end

    it "rechaza primary_key con espacios" do
      expect do
        described_class.new(base_options.merge(table_name: "versions", primary_key: "id desc"))
      end.to raise_error(DataDrain::ConfigurationError, /primary_key/)
    end

    it "rechaza table_name con punto (schema.table)" do
      expect do
        described_class.new(base_options.merge(table_name: "public.versions"))
      end.to raise_error(DataDrain::ConfigurationError, /table_name/)
    end

    it "acepta identificador válido con guión bajo y números" do
      expect do
        described_class.new(base_options.merge(table_name: "my_table_2", primary_key: "id_2"))
      end.not_to raise_error
    end

    it "acepta identificadores con mayúsculas" do
      expect do
        described_class.new(base_options.merge(table_name: "Versions", primary_key: "Id"))
      end.not_to raise_error
    end

    it "acepta identificador que empieza con guión bajo" do
      expect do
        described_class.new(base_options.merge(table_name: "_internal_table"))
      end.not_to raise_error
    end
  end

  it "skip export y purge cuando pg_count es 0" do
    allow(mock_duckdb).to receive(:query).with(/INSTALL postgres/)
    allow(mock_duckdb).to receive(:query).with(/SET max_memory|SET temp_directory|ATTACH/)
    allow(mock_duckdb).to receive(:query).with(/SELECT row_count FROM postgres_query/).and_return([[0]])

    expect(engine.call).to be true
  end

  it "skip_export omite export_to_parquet pero ejecuta verify_integrity" do
    engine = described_class.new(
      base_options.merge(
        table_name: "versions",
        skip_export: true,
        purge_where_clause: "created_at >= '2026-03-01'"
      )
    )
    allow(mock_duckdb).to receive(:query).with(/INSTALL postgres|SET max_memory|SET temp_directory|ATTACH/)
    allow(mock_duckdb).to receive(:query).with(/SELECT row_count FROM postgres_query/).and_return([[100]])
    allow(mock_duckdb).to receive(:query).with(/FROM read_parquet/).and_return([[100]])
    allow(mock_pg_conn).to receive(:exec).with(/SET idle_in_transaction_session_timeout/)
    allow(mock_pg_result).to receive(:cmd_tuples).and_return(100, 0)
    expect(mock_pg_conn).to receive(:exec).with(/DELETE FROM versions/).twice.and_return(mock_pg_result)

    expect(engine.call).to be true
  end

  it "setea idle_in_transaction_session_timeout = 0" do
    engine = described_class.new(base_options.merge(table_name: "versions",
                                                    purge_where_clause: "created_at >= '2026-03-01'"))
    allow(mock_duckdb).to receive(:query).with(/INSTALL postgres|SET max_memory|SET temp_directory|ATTACH/)
    allow(mock_duckdb).to receive(:query).with(/SELECT row_count FROM postgres_query/).and_return([[100]])
    allow(mock_duckdb).to receive(:query).with(/COPY \(/)
    allow(mock_duckdb).to receive(:query).with(/FROM read_parquet/).and_return([[100]])
    expect(mock_pg_conn).to receive(:exec).with(/SET idle_in_transaction_session_timeout = 0;/)
    allow(mock_pg_result).to receive(:cmd_tuples).and_return(100, 0)
    expect(mock_pg_conn).to receive(:exec).with(/DELETE FROM versions/).twice.and_return(mock_pg_result)

    engine.call
  end

  it "no setea idle_in_transaction_session_timeout cuando es nil" do
    DataDrain.configure { |c| c.idle_in_transaction_session_timeout = nil }
    engine = described_class.new(base_options.merge(table_name: "versions"))
    allow(mock_duckdb).to receive(:query).with(/INSTALL postgres|SET max_memory|SET temp_directory|ATTACH/)
    allow(mock_duckdb).to receive(:query).with(/SELECT row_count FROM postgres_query/).and_return([[100]])
    allow(mock_duckdb).to receive(:query).with(/COPY \(/)
    allow(mock_duckdb).to receive(:query).with(/FROM read_parquet/).and_return([[100]])
    expect(mock_pg_conn).not_to receive(:exec).with(/idle_in_transaction_session_timeout/)
    allow(mock_pg_result).to receive(:cmd_tuples).and_return(0)
    allow(mock_pg_conn).to receive(:exec).with(/DELETE FROM versions/).and_return(mock_pg_result)

    engine.call
  ensure
    DataDrain.reset_configuration!
  end

  it "loop de purge termina cuando cmd_tuples devuelve 0" do
    engine = described_class.new(base_options.merge(table_name: "versions",
                                                    purge_where_clause: "created_at >= '2026-03-01'"))
    allow(mock_duckdb).to receive(:query).with(/INSTALL postgres|SET max_memory|SET temp_directory|ATTACH/)
    allow(mock_duckdb).to receive(:query).with(/SELECT row_count FROM postgres_query/).and_return([[100]])
    allow(mock_duckdb).to receive(:query).with(/COPY \(/)
    allow(mock_duckdb).to receive(:query).with(/FROM read_parquet/).and_return([[100]])
    allow(mock_pg_conn).to receive(:exec).with(/SET idle_in_transaction_session_timeout/)
    allow(mock_pg_conn).to receive(:exec).with(/DELETE FROM versions/).and_return(mock_pg_result)

    values = [1, 1, 0]
    call_count = 0
    allow(mock_pg_result).to receive(:cmd_tuples) do
      call_count += 1
      values[call_count - 1]
    end

    engine.call

    expect(call_count).to eq(3)
  end

  it "ejecuta el flujo ETL completo si la integridad es exitosa" do
    engine = described_class.new(base_options.merge(table_name: "versions",
                                                    purge_where_clause: "created_at >= '2026-03-01'"))

    # 1. Setup
    expect(mock_duckdb).to receive(:query).with(/INSTALL postgres/).ordered
    allow(mock_duckdb).to receive(:query).with(/SET max_memory/)
    allow(mock_duckdb).to receive(:query).with(/SET temp_directory/)
    expect(mock_duckdb).to receive(:query).with(/ATTACH .* AS pg_source/).ordered

    # 2. Conteo en Postgres (Simulamos que hay 100 registros)
    expect(mock_duckdb).to receive(:query).with(/SELECT row_count FROM postgres_query/).ordered.and_return([[100]])

    # 3. Exportación a Parquet
    expect(mock_duckdb).to receive(:query).with(/COPY \(/).ordered

    # 4. Verificación de Integridad (Simulamos que el Parquet también tiene 100 registros)
    expect(mock_duckdb).to receive(:query).with(/FROM read_parquet/).ordered.and_return([[100]])

    # 5. Purga en Postgres
    allow(mock_pg_conn).to receive(:exec).with(/SET idle_in_transaction_session_timeout/)
    allow(mock_pg_result).to receive(:cmd_tuples).and_return(100, 0)
    expect(mock_pg_conn).to receive(:exec).with(/DELETE FROM versions/).twice.and_return(mock_pg_result)

    expect(engine.call).to be true
  end

  it "aborta la purga y retorna false si la integridad falla" do
    # Ignoramos los querys de setup
    allow(mock_duckdb).to receive(:query).with(/INSTALL postgres|SET max_memory|SET temp_directory|ATTACH/)

    # Postgres dice que hay 100
    allow(mock_duckdb).to receive(:query).with(/SELECT row_count FROM postgres_query/).and_return([[100]])

    # Exportación
    allow(mock_duckdb).to receive(:query).with(/COPY \(/)

    # 💡 Parquet dice que solo hay 99 (Simulamos un error de integridad)
    allow(mock_duckdb).to receive(:query).with(/FROM read_parquet/).and_return([[99]])

    # Garantizamos que NUNCA se llame a la eliminación en Postgres
    expect(mock_pg_conn).not_to receive(:exec).with(/DELETE FROM versions/)

    expect(engine.call).to be false
  end

  describe "VACUUM post-purge" do
    it "no ejecuta VACUUM cuando vacuum_after_purge es false (default)" do
      engine = described_class.new(base_options.merge(table_name: "versions"))

      allow(mock_duckdb).to receive(:query).with(/INSTALL postgres|SET max_memory|SET temp_directory|ATTACH/)
      allow(mock_duckdb).to receive(:query).with(/SELECT row_count FROM postgres_query/).and_return([[100]])
      allow(mock_duckdb).to receive(:query).with(/COPY \(/)
      allow(mock_duckdb).to receive(:query).with(/FROM read_parquet/).and_return([[100]])
      allow(mock_pg_conn).to receive(:exec).with(/SET idle_in_transaction_session_timeout/)
      allow(mock_pg_result).to receive(:cmd_tuples).and_return(0)
      allow(mock_pg_conn).to receive(:exec).with(/DELETE FROM versions/).and_return(mock_pg_result)

      expect(mock_pg_conn).not_to receive(:exec).with(/VACUUM/)
      engine.call
    end

    it "ejecuta VACUUM ANALYZE cuando vacuum_after_purge es true y hay deletes" do
      DataDrain.configure { |c| c.vacuum_after_purge = true }
      engine = described_class.new(base_options.merge(table_name: "versions",
                                                      purge_where_clause: "created_at >= '2026-03-01'"))

      allow(mock_duckdb).to receive(:query).with(/INSTALL postgres|SET max_memory|SET temp_directory|ATTACH/)
      allow(mock_duckdb).to receive(:query).with(/SELECT row_count FROM postgres_query/).and_return([[100]])
      allow(mock_duckdb).to receive(:query).with(/COPY \(/)
      allow(mock_duckdb).to receive(:query).with(/FROM read_parquet/).and_return([[100]])
      allow(mock_pg_conn).to receive(:exec).with(/SET idle_in_transaction_session_timeout/)
      allow(mock_pg_result).to receive(:cmd_tuples).and_return(100, 0)
      allow(mock_pg_conn).to receive(:exec).with(/DELETE FROM versions/).and_return(mock_pg_result)

      dead_result = instance_double(PG::Result)
      allow(dead_result).to receive(:first).and_return({ "n_dead_tup" => "500" })
      allow(mock_pg_conn).to receive(:exec_params).with(/pg_stat_user_tables/, anything).and_return(dead_result)
      expect(mock_pg_conn).to receive(:exec).with(/VACUUM ANALYZE versions/)

      engine.call
    ensure
      DataDrain.reset_configuration!
    end

    it "no ejecuta VACUUM si total_deleted es 0" do
      DataDrain.configure { |c| c.vacuum_after_purge = true }
      engine = described_class.new(base_options.merge(table_name: "versions"))

      allow(mock_duckdb).to receive(:query).with(/INSTALL postgres|SET max_memory|SET temp_directory|ATTACH/)
      allow(mock_duckdb).to receive(:query).with(/SELECT row_count FROM postgres_query/).and_return([[100]])
      allow(mock_duckdb).to receive(:query).with(/COPY \(/)
      allow(mock_duckdb).to receive(:query).with(/FROM read_parquet/).and_return([[100]])
      allow(mock_pg_conn).to receive(:exec).with(/SET idle_in_transaction_session_timeout/)
      allow(mock_pg_result).to receive(:cmd_tuples).and_return(0)
      allow(mock_pg_conn).to receive(:exec).with(/DELETE FROM versions/).and_return(mock_pg_result)

      expect(mock_pg_conn).not_to receive(:exec).with(/VACUUM/)

      engine.call
    ensure
      DataDrain.reset_configuration!
    end

    it "captura PG::Error y loguea engine.vacuum_error sin levantar" do
      DataDrain.configure { |c| c.vacuum_after_purge = true }
      engine = described_class.new(base_options.merge(table_name: "versions"))

      allow(mock_duckdb).to receive(:query).with(/INSTALL postgres|SET max_memory|SET temp_directory|ATTACH/)
      allow(mock_duckdb).to receive(:query).with(/SELECT row_count FROM postgres_query/).and_return([[100]])
      allow(mock_duckdb).to receive(:query).with(/COPY \(/)
      allow(mock_duckdb).to receive(:query).with(/FROM read_parquet/).and_return([[100]])
      allow(mock_pg_conn).to receive(:exec).with(/SET idle_in_transaction_session_timeout/)
      allow(mock_pg_result).to receive(:cmd_tuples).and_return(100, 0)
      allow(mock_pg_conn).to receive(:exec).with(/DELETE FROM versions/).and_return(mock_pg_result)

      dead_result = instance_double(PG::Result)
      allow(dead_result).to receive(:first).and_return({ "n_dead_tup" => "500" })
      allow(mock_pg_conn).to receive(:exec_params).with(/pg_stat_user_tables/, anything).and_return(dead_result)
      allow(mock_pg_conn).to receive(:exec).with(/VACUUM ANALYZE/).and_raise(PG::Error, "lock timeout")

      expect { engine.call }.not_to raise_error
    ensure
      DataDrain.reset_configuration!
    end
  end

  describe "warning de purga lenta" do
    it "handle_batch_timing emite slow_batch cuando batch excede threshold" do
      DataDrain.configure do |c|
        c.slow_batch_threshold_s = 5
        c.slow_batch_alert_after = 3
      end
      engine = described_class.new(base_options.merge(table_name: "versions"))

      allow(engine).to receive(:safe_log).with(:warn, "engine.slow_batch", anything).and_call_original
      expect(engine).to receive(:safe_log).with(:warn, "engine.slow_batch",
                                                hash_including(batch_duration_s: a_value > 5, streak: 1))

      engine.send(:handle_batch_timing, 10.0, 100, 0)
    ensure
      DataDrain.reset_configuration!
    end

    it "handle_batch_timing emite purge_degraded tras N lotes lentos consecutivos" do
      DataDrain.configure do |c|
        c.slow_batch_threshold_s = 5
        c.slow_batch_alert_after = 3
      end
      engine = described_class.new(base_options.merge(table_name: "versions"))

      allow(engine).to receive(:safe_log).and_call_original
      expect(engine).to receive(:safe_log).with(:warn, "engine.purge_degraded", anything).once

      streak = 0
      3.times do
        streak = engine.send(:handle_batch_timing, 10.0, 100, streak)
      end
    ensure
      DataDrain.reset_configuration!
    end

    it "resetea streak si un lote es rápido" do
      DataDrain.configure do |c|
        c.slow_batch_threshold_s = 5
        c.slow_batch_alert_after = 3
      end
      engine = described_class.new(base_options.merge(table_name: "versions"))

      allow(mock_duckdb).to receive(:query).with(/INSTALL postgres|SET max_memory|SET temp_directory|ATTACH/)
      allow(mock_duckdb).to receive(:query).with(/SELECT row_count FROM postgres_query/).and_return([[100]])
      allow(mock_duckdb).to receive(:query).with(/COPY \(/)
      allow(mock_duckdb).to receive(:query).with(/FROM read_parquet/).and_return([[100]])
      allow(mock_pg_conn).to receive(:exec).with(/SET idle_in_transaction_session_timeout/)

      allow(mock_pg_result).to receive(:cmd_tuples).and_return(100, 0)
      allow(mock_pg_conn).to receive(:exec).with(/DELETE FROM versions/).and_return(mock_pg_result)

      expect(DataDrain.configuration.logger).not_to receive(:warn).with(/engine.purge_degraded/)

      engine.call
    ensure
      DataDrain.reset_configuration!
    end
  end

  describe "purge_where_clause" do
    let(:base_options_with_purge) do
      {
        bucket: bucket,
        start_date: Time.new(2026, 3, 1),
        end_date: Time.new(2026, 3, 31),
        partition_keys: %w[year month],
        table_name: "versions",
        where_clause: "isp_id IS NOT NULL"
      }
    end

    it "purges using where_clause when purge_where_clause not provided (backwards compatible)" do
      engine = described_class.new(base_options_with_purge)

      allow(mock_duckdb).to receive(:query).with(/INSTALL postgres|SET max_memory|SET temp_directory|ATTACH/)
      allow(mock_duckdb).to receive(:query).with(/SELECT row_count FROM postgres_query/).and_return([[10]])
      allow(mock_duckdb).to receive(:query).with(/COPY \(/)
      allow(mock_duckdb).to receive(:query).with(/FROM read_parquet/).and_return([[10]])
      allow(mock_pg_conn).to receive(:exec).with(/SET idle_in_transaction_session_timeout/)

      expect(mock_pg_conn).to receive(:exec).with(/DELETE FROM versions/).twice.and_return(mock_pg_result)
      allow(mock_pg_result).to receive(:cmd_tuples).and_return(10, 0)

      expect(engine.call).to be true
    end

    it "purges all when purge_where_clause is empty (no extra filter)" do
      engine = described_class.new(
        base_options_with_purge.merge(purge_where_clause: "")
      )

      allow(mock_duckdb).to receive(:query).with(/INSTALL postgres|SET max_memory|SET temp_directory|ATTACH/)
      allow(mock_duckdb).to receive(:query).with(/SELECT row_count FROM postgres_query/).and_return([[10]])
      allow(mock_duckdb).to receive(:query).with(/COPY \(/)
      allow(mock_duckdb).to receive(:query).with(/FROM read_parquet/).and_return([[10]])

      allow(mock_pg_conn).to receive(:exec).with(/SET idle_in_transaction_session_timeout/)
      allow(mock_pg_result).to receive(:cmd_tuples).and_return(10, 0)
      expect(mock_pg_conn).to receive(:exec).with(/DELETE FROM versions/).twice.and_return(mock_pg_result)

      expect(engine.call).to be true
    end

    it "integrity check uses base_where_sql, not purge_where_clause" do
      engine = described_class.new(
        base_options_with_purge.merge(purge_where_clause: "status = 'deleted'")
      )

      allow(mock_duckdb).to receive(:query).with(/INSTALL postgres|SET max_memory|SET temp_directory|ATTACH/)
      expect(mock_duckdb).to receive(:query).with(/isp_id IS NOT NULL/).at_least(:once).and_return([[10]])
      expect(mock_duckdb).not_to receive(:query).with(/status = 'deleted'/)
      allow(mock_duckdb).to receive(:query).with(/COPY \(/)
      allow(mock_duckdb).to receive(:query).with(/FROM read_parquet/).and_return([[10]])

      allow(mock_pg_conn).to receive(:exec).with(/SET idle_in_transaction_session_timeout/)
      allow(mock_pg_result).to receive(:cmd_tuples).and_return(5, 0)
      expect(mock_pg_conn).to receive(:exec).with(/DELETE FROM versions.*status = 'deleted'/m).twice.and_return(mock_pg_result)

      expect(engine.call).to be true
    end

    it "archives subset but purges superset (primary use case)" do
      engine = described_class.new(
        base_options_with_purge.merge(
          where_clause: "isp_id IS NOT NULL",
          purge_where_clause: ""
        )
      )

      allow(mock_duckdb).to receive(:query).with(/INSTALL postgres|SET max_memory|SET temp_directory|ATTACH/)
      expect(mock_duckdb).to receive(:query).with(/isp_id IS NOT NULL/).at_least(:once).and_return([[10]])
      allow(mock_duckdb).to receive(:query).with(/COPY \(/)
      allow(mock_duckdb).to receive(:query).with(/FROM read_parquet/).and_return([[10]])

      allow(mock_pg_conn).to receive(:exec).with(/SET idle_in_transaction_session_timeout/)
      allow(mock_pg_result).to receive(:cmd_tuples).and_return(10, 0)
      expect(mock_pg_conn).to receive(:exec).with(/DELETE FROM versions/).twice do |sql|
        expect(sql).not_to include("isp_id IS NOT NULL")
        mock_pg_result
      end

      expect(engine.call).to be true
    end

    it "purge_where_clause independent of where_clause" do
      engine = described_class.new(
        base_options_with_purge.merge(
          where_clause: "isp_id IS NOT NULL",
          purge_where_clause: "status = 'deleted'"
        )
      )

      allow(mock_duckdb).to receive(:query).with(/INSTALL postgres|SET max_memory|SET temp_directory|ATTACH/)
      allow(mock_duckdb).to receive(:query).with(/SELECT row_count FROM postgres_query/).and_return([[10]])
      allow(mock_duckdb).to receive(:query).with(/COPY \(/)
      allow(mock_duckdb).to receive(:query).with(/FROM read_parquet/).and_return([[10]])

      allow(mock_pg_conn).to receive(:exec).with(/SET idle_in_transaction_session_timeout/)
      allow(mock_pg_result).to receive(:cmd_tuples).and_return(5, 0)
      expect(mock_pg_conn).to receive(:exec).with(/DELETE FROM versions.*status = 'deleted'/m).twice.and_return(mock_pg_result)

      expect(engine.call).to be true
    end
  end
end
