# frozen_string_literal: true

require "active_model"
require "duckdb"

module DataDrain
  # Clase base que actúa como un ORM (Object-Relational Mapper) de solo lectura y purga
  # para interactuar con el Data Lake en formato Parquet utilizando DuckDB.
  #
  # @abstract Subclasifica este modelo para cada tabla archivada.
  # @example
  #   class ArchivedVersion < DataDrain::Record
  #     self.folder_name = 'versions'
  #     self.partition_keys = [:isp_id, :year, :month]
  #     attribute :event, :string
  #   end
  class Record
    include ActiveModel::Model
    include ActiveModel::Attributes
    extend Observability
    private_class_method :safe_log, :exception_metadata, :observability_name

    class_attribute :bucket
    class_attribute :folder_name
    class_attribute :partition_keys

    # Cierra la conexión DuckDB del thread actual y limpia Thread.current.
    # Idempotente: llamarlo varias veces no levanta.
    #
    # Útil en middlewares de Sidekiq/Puma para evitar memory leak en threads
    # de larga vida.
    #
    # @return [void]
    def self.disconnect!
      entry = Thread.current[:data_drain_duckdb]
      Thread.current[:data_drain_duckdb] = nil
      return unless entry

      entry[:conn]&.close
      entry[:db]&.close
    rescue StandardError
      nil
    end

    # Retorna la conexión persistente a DuckDB en memoria para el hilo (Thread) actual.
    # Esto previene tener que recargar extensiones (como httpfs) en cada consulta.
    #
    # @return [DuckDB::Connection] Conexión activa a DuckDB.
    def self.connection
      Thread.current[:data_drain_duckdb] ||= begin
        db = DuckDB::Database.open(":memory:")
        conn = db.connect

        config = DataDrain.configuration
        conn.query("SET max_memory='#{config.limit_ram}';") if config.limit_ram.present?
        conn.query("SET temp_directory='#{config.tmp_directory}'") if config.tmp_directory.present?

        DataDrain::Storage.adapter.setup_duckdb(conn)

        conn.query("SET lock_configuration=true;")

        { db: db, conn: conn }
      end
      Thread.current[:data_drain_duckdb][:conn]
    end

    # Consulta registros en el Data Lake filtrando por claves de partición.
    #
    # @param limit [Integer] Cantidad máxima de registros a retornar.
    # @param partitions [Hash] Pares clave-valor correspondientes a las particiones.
    # @return [Array<DataDrain::Record>] Colección de registros instanciados.
    def self.where(limit: 50, **partitions)
      path = build_query_path(partitions)

      sql = <<~SQL
        SELECT #{attribute_names.join(", ")}
        FROM read_parquet('#{path}')
        ORDER BY created_at DESC
        LIMIT #{limit}
      SQL

      execute_and_instantiate(sql, attribute_names)
    end

    # Busca un registro específico por su ID.
    # Implementa sanitización básica para prevenir Inyección SQL.
    #
    # @param id [String, Integer] Identificador único del registro.
    # @param partitions [Hash] Pares clave-valor de las particiones donde buscar.
    # @return [DataDrain::Record, nil] El registro encontrado o nil.
    def self.find(id, **partitions)
      path = build_query_path(partitions)
      # Sanitización básica: duplicar comillas simples para anular escapes SQL
      safe_id = id.to_s.gsub("'", "''")

      sql = <<~SQL
        SELECT #{attribute_names.join(", ")}
        FROM read_parquet('#{path}')
        WHERE id = '#{safe_id}'
        LIMIT 1
      SQL

      execute_and_instantiate(sql, attribute_names).first
    end

    # Elimina físicamente los directorios o prefijos de S3.
    #
    # @param partitions [Hash] Particiones a eliminar.
    # @return [Integer] Cantidad de particiones físicas eliminadas.
    def self.destroy_all(**partitions)
      adapter = DataDrain::Storage.adapter
      @logger = DataDrain.configuration.logger
      safe_log(:info, "record.destroy_all", { folder: folder_name, partitions: partitions.inspect })

      adapter.destroy_partitions(bucket, folder_name, partition_keys, partitions)
    end

    # @return [String] Representación legible en consola.
    def inspect
      inspection = attributes.map do |name, value|
        "#{name}: #{value.nil? ? "nil" : value.inspect}"
      end.compact.join(", ")

      "#<#{self.class} #{inspection}>"
    end

    class << self
      private

      # @api private
      # @param partitions [Hash]
      # @return [String]
      def build_query_path(partitions)
        partition_path = partition_keys.map do |k|
          val = partitions.key?(k.to_sym) ? partitions[k.to_sym] : partitions[k.to_s]
          val.nil? || val.to_s.empty? ? "#{k}=*" : "#{k}=#{val}"
        end.join("/")
        DataDrain::Storage.adapter.build_path(bucket, folder_name, partition_path)
      end

      # @api private
      # @param sql [String]
      # @param columns [Array<String>]
      # @return [Array<DataDrain::Record>]
      def execute_and_instantiate(sql, columns)
        @logger = DataDrain.configuration.logger
        result = connection.query(sql)
        result.map { |row| new(columns.zip(row).to_h) }
      rescue DuckDB::Error => e
        safe_log(:warn, "record.parquet_not_found", exception_metadata(e))
        []
      end
    end
  end
end
