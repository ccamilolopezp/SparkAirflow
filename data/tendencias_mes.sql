WITH monthly_trends AS (
    SELECT 
        DATE_TRUNC('month', fa.published_at) AS month,
        dt.name AS topic,
        COUNT(*) AS article_count
    FROM spacenews.fact_article fa
    JOIN spacenews.dim_topic dt ON fa.topic_id = dt.topic_id
    GROUP BY month, topic
)
SELECT 
    month, 
    topic, 
    article_count,
    RANK() OVER (PARTITION BY month ORDER BY article_count DESC) AS rank
FROM monthly_trends
ORDER BY month DESC, rank;
