WITH source_influence AS (
    SELECT 
        dns.name AS source_name,
        COUNT(fa.article_id) AS total_articles,
        AVG(fa.importance_score) AS avg_importance
    FROM spacenews.fact_article fa
    JOIN spacenews.dim_news_source dns ON fa.source_id = dns.source_id
    GROUP BY source_name
)
SELECT 
    source_name, 
    total_articles, 
    avg_importance,
    RANK() OVER (ORDER BY total_articles DESC, avg_importance DESC) AS rank
FROM source_influence
ORDER BY rank;
