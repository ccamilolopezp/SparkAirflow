WITH monthly_trends AS (
    -- Agrega una columna de mes y cuenta los artículos por tema en cada mes
    SELECT 
        DATE_TRUNC('month', fa.published_at) AS month,  -- Agrupa por mes
        dt.name AS topic,  -- Nombre del tema
        COUNT(*) AS article_count  -- Número de artículos en ese tema
    FROM spacenews.fact_article fa
    JOIN spacenews.dim_topic dt ON fa.topic_id = dt.topic_id
    GROUP BY month, topic
)

-- Asigna un ranking a los temas según su cantidad de artículos por mes
SELECT 
    month, 
    topic, 
    article_count,
    RANK() OVER (PARTITION BY month ORDER BY article_count DESC) AS rank  -- Ranking por mes
FROM monthly_trends
ORDER BY month DESC, rank;  -- Ordena por mes (más reciente primero) y ranking dentro de cada mes
