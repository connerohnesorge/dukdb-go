.PHONY: test test-short test-iceberg test-iceberg-cloud test-iceberg-spark test-iceberg-flink
.PHONY: docker-up docker-down docker-spark docker-flink

# Run all tests
test:
	go test ./...

# Run tests in short mode (skip slow tests)
test-short:
	go test -short ./...

# Run all Iceberg tests
test-iceberg:
	go test -v ./internal/io/iceberg/...

# Run Iceberg cloud storage tests (requires Docker)
test-iceberg-cloud:
	@echo "Starting MinIO and fake GCS..."
	cd internal/io/iceberg/testdata && docker compose up -d minio minio-setup fake-gcs
	@echo "Waiting for services to be ready..."
	@sleep 10
	@echo "Running cloud storage tests..."
	go test -v ./internal/io/iceberg -run TestCloudStorage
	@echo "Stopping services..."
	cd internal/io/iceberg/testdata && docker compose down

# Run Spark compatibility tests (requires Docker)
test-iceberg-spark:
	@echo "Starting Spark..."
	cd internal/io/iceberg/testdata && docker compose up -d spark
	@echo "Waiting for Spark to initialize..."
	@sleep 20
	@echo "Generating Spark tables..."
	docker exec iceberg-spark /opt/spark/bin/spark-submit \
		--master local[*] \
		--packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.4 \
		/opt/spark-scripts/generate_iceberg_tables.py || true
	@echo "Running Spark compatibility tests..."
	go test -v ./internal/io/iceberg -run TestSparkGenerated
	@echo "Stopping Spark..."
	cd internal/io/iceberg/testdata && docker compose down

# Run Flink compatibility tests (requires Docker)
test-iceberg-flink:
	@echo "Starting Flink..."
	cd internal/io/iceberg/testdata && docker compose up -d flink-jobmanager flink-taskmanager
	@echo "Waiting for Flink to initialize..."
	@sleep 25
	@echo "Generating Flink tables..."
	docker exec iceberg-flink-jobmanager /opt/flink/bin/sql-client.sh \
		-f /opt/flink-scripts/generate_iceberg_tables.sql || true
	@echo "Running Flink compatibility tests..."
	go test -v ./internal/io/iceberg -run TestFlinkGenerated
	@echo "Stopping Flink..."
	cd internal/io/iceberg/testdata && docker compose down

# Start all Docker services
docker-up:
	cd internal/io/iceberg/testdata && docker compose up -d

# Stop all Docker services
docker-down:
	cd internal/io/iceberg/testdata && docker compose down -v

# Start only Spark
docker-spark:
	cd internal/io/iceberg/testdata && docker compose up -d spark

# Start only Flink
docker-flink:
	cd internal/io/iceberg/testdata && docker compose up -d flink-jobmanager flink-taskmanager

# Run all Iceberg compatibility tests (cloud + Spark + Flink)
test-iceberg-full: test-iceberg-cloud test-iceberg-spark test-iceberg-flink
	@echo "✅ All Iceberg compatibility tests completed"

