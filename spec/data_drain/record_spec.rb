# frozen_string_literal: true

RSpec.describe DataDrain::Record do
  let(:bucket) { "spec/fixtures" }

  before(:all) do
    path = "spec/fixtures/test_archive"
    FileUtils.rm_rf(path)
    db = DuckDB::Database.open(":memory:")
    conn = db.connect
    conn.query(<<~SQL)
      COPY (
        SELECT 'uuid-1' AS id, 100 AS value, '2026-03-01'::TIMESTAMP AS created_at, 2026 AS year, 3 AS month
        UNION ALL SELECT 'uuid-2', 200, '2026-03-15'::TIMESTAMP, 2026, 3
        UNION ALL SELECT 'uuid-3', 300, '2025-12-01'::TIMESTAMP, 2025, 12
      ) TO '#{path}' (FORMAT PARQUET, PARTITION_BY (year, month), OVERWRITE_OR_IGNORE 1);
    SQL
    conn.close
    db.close
  end

  after(:all) do
    FileUtils.rm_rf("spec/fixtures/test_archive")
  end

  let(:record_class) do
    Class.new(described_class) do
      self.bucket = "spec/fixtures"
      self.folder_name = "test_archive"
      self.partition_keys = %i[year month]

      attribute :id, :string
      attribute :value, :integer
      attribute :created_at, :datetime
    end
  end

  describe ".connection" do
    after do
      described_class.disconnect!
    end

    it "retorna DuckDB::Connection" do
      conn = record_class.connection
      expect(conn).to be_a(DuckDB::Connection)
    end

    it "cachea la conexion en el mismo thread" do
      conn1 = record_class.connection
      conn2 = record_class.connection
      expect(conn1).to be(conn2)
    end

    it "retorna conexion diferente en threads distintos" do
      conn1 = record_class.connection
      conn2 = nil

      thread = Thread.new { conn2 = record_class.connection }
      thread.join

      expect(conn2).not_to be(conn1)
    ensure
      thread.join if thread.alive?
    end

    it "aplica lock_configuration=true tras setup" do
      conn = record_class.connection
      expect do
        conn.query("SET memory_limit='1KB';")
      end.to raise_error(DuckDB::Error, /lock/i)
    end

    it "reaplica lock_configuration tras reconnect" do
      record_class.connection
      record_class.disconnect!
      conn = record_class.connection

      expect do
        conn.query("SET memory_limit='1KB';")
      end.to raise_error(DuckDB::Error, /lock/i)
    end
  end

  describe ".disconnect!" do
    after do
      described_class.disconnect!
    end

    it "limpia Thread.current" do
      record_class.connection
      expect(Thread.current[:data_drain_duckdb]).not_to be_nil
      record_class.disconnect!
      expect(Thread.current[:data_drain_duckdb]).to be_nil
    end

    it "es idempotente" do
      expect { record_class.disconnect! }.not_to raise_error
      expect { record_class.disconnect! }.not_to raise_error
    end

    it "permite reconnectar despues de disconnect" do
      conn1 = record_class.connection
      record_class.disconnect!
      conn2 = record_class.connection
      expect(conn1).not_to be(conn2)
    end
  end

  describe ".where" do
    after do
      described_class.disconnect!
    end

    it "retorna instancias que matchean las particiones" do
      results = record_class.where(year: 2026, month: 3)
      expect(results.size).to eq(2)
      expect(results.map(&:id).sort).to eq(%w[uuid-1 uuid-2])
    end

    it "acepta kwargs en cualquier orden" do
      results = record_class.where(month: 3, year: 2026)
      expect(results.size).to eq(2)
    end

    it "respeta limit" do
      results = record_class.where(year: 2026, month: 3, limit: 1)
      expect(results.size).to eq(1)
    end

    it "retorna array vacio si no hay match" do
      results = record_class.where(year: 2099, month: 12)
      expect(results).to eq([])
    end

    it "retorna array vacio si el directorio no existe" do
      klass = Class.new(described_class) do
        self.bucket = "spec/fixtures"
        self.folder_name = "nonexistent_archive"
        self.partition_keys = %i[year month]
        attribute :id, :string
      end
      results = klass.where(year: 2026, month: 3)
      expect(results).to eq([])
    end
  end

  describe ".find" do
    after do
      described_class.disconnect!
    end

    it "retorna instancia si existe" do
      result = record_class.find("uuid-1", year: 2026, month: 3)
      expect(result).not_to be_nil
      expect(result.id).to eq("uuid-1")
      expect(result.value).to eq(100)
    end

    it "retorna nil si no existe" do
      result = record_class.find("nonexistent", year: 2026, month: 3)
      expect(result).to be_nil
    end

    it "sanitiza id con comillas simples" do
      result = record_class.find("uuid-1' OR 1=1 --", year: 2026, month: 3)
      expect(result).to be_nil
    end
  end

  describe ".destroy_all" do
    it "delega al adapter" do
      mock_adapter = instance_double("DataDrain::Storage::Base")
      allow(DataDrain::Storage).to receive(:adapter).and_return(mock_adapter)
      allow(DataDrain).to receive(:configuration).and_return(
        double("config", logger: Logger.new(StringIO.new))
      )

      expect(mock_adapter).to receive(:destroy_partitions)
        .with("spec/fixtures", "test_archive", %i[year month], { year: 2026, month: 3 })
        .and_return(1)

      record_class.destroy_all(year: 2026, month: 3)
    end
  end

  describe ".build_query_path" do
    it "arma path con partition_keys en orden de la clase" do
      path = record_class.send(:build_query_path, { month: 3, year: 2026 })
      expect(path).to include("year=2026")
      expect(path).to include("month=3")
    end

    it "soporta symbol keys" do
      path = record_class.send(:build_query_path, { year: :integer, month: :integer })
      expect(path).to include("year=integer")
    end

    it "acepta string keys en el hash de particiones" do
      path = record_class.send(:build_query_path, { "year" => 2026, "month" => 3 })
      expect(path).to include("year=2026")
      expect(path).to include("month=3")
    end

    it "combina string keys y symbol keys en el mismo hash" do
      path = record_class.send(:build_query_path, { "year" => 2026, month: 3 })
      expect(path).to include("year=2026")
      expect(path).to include("month=3")
    end

    it "usa wildcard cuando falta una partition key" do
      path = record_class.send(:build_query_path, { year: 2026 })
      expect(path).to include("year=2026")
      expect(path).to include("month=*")
    end

    it "usa wildcards para todas las partition keys faltantes" do
      path = record_class.send(:build_query_path, {})
      expect(path).to include("year=*")
      expect(path).to include("month=*")
    end

    it "trata valor cero (falsy) como valor legitimo, no como ausente" do
      path = record_class.send(:build_query_path, { year: 2026, month: 0 })
      expect(path).to include("year=2026")
      expect(path).to include("month=0")
    end
  end
end
