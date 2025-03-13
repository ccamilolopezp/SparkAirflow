-- Crear el esquema si no existe
DO $$ 
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'spacenews') THEN
        CREATE SCHEMA spacenews;
    END IF;
END $$;

-- Asegurar que estamos usando el esquema correcto
SET search_path TO spacenews;

-- Crear la tabla dim_news_source con source_id autogenerado y name único
CREATE TABLE IF NOT EXISTS spacenews.dim_news_source (
    source_id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL UNIQUE,
    url VARCHAR(255) NOT NULL,
    reliability_score FLOAT CHECK (reliability_score BETWEEN 0 AND 1)
);

-- Crear la tabla dim_topic con clave primaria
CREATE TABLE IF NOT EXISTS spacenews.dim_topic (
    topic_id INT PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(255) NOT NULL
);

-- Crear la tabla fact_article con particionamiento
CREATE TABLE IF NOT EXISTS spacenews.fact_article (
    article_id INT,
    source_id INT NOT NULL,
    topic_id INT NOT NULL,
    published_at TIMESTAMP NOT NULL,
    sentiment_score FLOAT,
    importance_score FLOAT,
    PRIMARY KEY (article_id, published_at),
    CONSTRAINT fk_fact_article_source FOREIGN KEY (source_id) REFERENCES spacenews.dim_news_source(source_id) ON DELETE CASCADE,
    CONSTRAINT fk_fact_article_topic FOREIGN KEY (topic_id) REFERENCES spacenews.dim_topic(topic_id) ON DELETE CASCADE
)
PARTITION BY RANGE (published_at);

--Se crean particiones para 2025 si se requiere años anteriores cambiar año y volver a ejecutar.
DO $$ 
DECLARE 
    year INT := 2025; -- CAMBIAR EL AÑO
    month INT;
    partition_name TEXT;
    start_date DATE;
    end_date DATE;
BEGIN
    FOR month IN 1..12 LOOP
        partition_name := 'fact_article_' || year || '_' || LPAD(month::TEXT, 2, '0');
        start_date := DATE (year || '-01-01') + INTERVAL '1 month' * (month - 1);
        end_date := start_date + INTERVAL '1 month';

        IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = partition_name AND table_schema = 'spacenews') THEN
            EXECUTE FORMAT(
                'CREATE TABLE spacenews.%I PARTITION OF spacenews.fact_article
                 FOR VALUES FROM (%L) TO (%L);',
                partition_name, start_date, end_date
            );
        END IF;
    END LOOP;
END $$;

-- Crear índices optimizados
CREATE INDEX IF NOT EXISTS idx_fact_article_published_at ON spacenews.fact_article(published_at);
CREATE INDEX IF NOT EXISTS idx_fact_article_source ON spacenews.fact_article(source_id);
CREATE INDEX IF NOT EXISTS idx_fact_article_topic ON spacenews.fact_article(topic_id);
