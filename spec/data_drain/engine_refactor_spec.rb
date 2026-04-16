# frozen_string_literal: true

RSpec.describe DataDrain::Engine, "refactor step extraction" do
  let(:bucket) { "tmp/test_lake" }
  let(:base_options) do
    {
      bucket: bucket,
      start_date: Time.new(2026, 3, 1),
      end_date: Time.new(2026, 3, 31),
      partition_keys: %w[year month]
    }
  end

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

  describe "#purge_loop (refactor)" do
    it "retorna total_deleted (suma de cmd_tuples por lote)" do
      engine = described_class.new(
        base_options.merge(
          table_name: "versions",
          purge_where_clause: "created_at >= '2026-03-01'"
        )
      )

      values = [100, 50, 0]
      call_count = 0
      allow(mock_pg_result).to receive(:cmd_tuples) do
        call_count += 1
        values[call_count - 1]
      end
      allow(mock_pg_conn).to receive(:exec).with(/DELETE FROM versions/).and_return(mock_pg_result)

      total = engine.send(:purge_loop, mock_pg_conn)
      expect(total).to eq(150)
    end

    it "retorna 0 cuando no hay filas para borrar" do
      engine = described_class.new(
        base_options.merge(
          table_name: "versions",
          purge_where_clause: "created_at >= '2026-03-01'"
        )
      )

      allow(mock_pg_result).to receive(:cmd_tuples).and_return(0)
      allow(mock_pg_conn).to receive(:exec).with(/DELETE FROM versions/).and_return(mock_pg_result)

      total = engine.send(:purge_loop, mock_pg_conn)
      expect(total).to eq(0)
    end
  end

  describe "#durations hash" do
    it "acumula timings en @durations" do
      engine = described_class.new(
        base_options.merge(
          table_name: "versions",
          purge_where_clause: "created_at >= '2026-03-01'"
        )
      )

      allow(mock_duckdb).to receive(:query).with(/INSTALL postgres|SET max_memory|SET temp_directory|ATTACH/)
      allow(mock_duckdb).to receive(:query).with(/SELECT row_count FROM postgres_query/).and_return([[0]])
      allow(mock_pg_result).to receive(:cmd_tuples).and_return(0)
      allow(mock_pg_conn).to receive(:exec).with(/DELETE FROM versions/).and_return(mock_pg_result)

      engine.call

      expect(engine.instance_variable_get(:@durations)).to have_key(:db_query)
    end
  end

  describe "#timed helper" do
    it "guarda la duración del bloque en @durations" do
      engine = described_class.new(
        base_options.merge(
          table_name: "versions",
          purge_where_clause: "created_at >= '2026-03-01'"
        )
      )
      engine.instance_variable_set(:@durations, {})

      result = engine.send(:timed, :test_step) { 42 }

      expect(engine.instance_variable_get(:@durations)).to have_key(:test_step)
      expect(engine.instance_variable_get(:@durations)[:test_step]).to be_a(Float)
      expect(result).to eq(42)
    end
  end
end
