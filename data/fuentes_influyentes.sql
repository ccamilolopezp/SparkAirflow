WITH source_influence AS (
    -- Calcula la cantidad total de artículos y el puntaje de importancia promedio por fuente
    SELECT 
        dns.name AS source_name,  -- Nombre de la fuente
        COUNT(fa.article_id) AS total_articles,  -- Total de artículos publicados por la fuente
        AVG(fa.importance_score) AS avg_importance  -- Promedio del puntaje de importancia
    FROM spacenews.fact_article fa
    JOIN spacenews.dim_news_source dns ON fa.source_id = dns.source_id
    GROUP BY source_name
)

-- Asigna un ranking a las fuentes basado en la cantidad de artículos y el puntaje de importancia
SELECT 
    source_name, 
    total_articles, 
    avg_importance,
    RANK() OVER (ORDER BY total_articles DESC, avg_importance DESC) AS rank  -- Ranking según influencia
FROM source_influence
ORDER BY rank;  -- Ordena por el ranking calculado
