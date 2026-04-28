-- All comics have creation cost
SELECT COUNT(*) as missing_cost 
FROM skip-comics.xkcd_marts.dim_comic
WHERE creation_cost IS NULL;

-- Views are in valid range
SELECT COUNT(*) as invalid_views 
FROM skip-comics.xkcd_marts.fact_comic
WHERE view_count < 0 OR view_count > 10000;

-- Reviews are 1-10
SELECT COUNT(*) as invalid_reviews 
FROM skip-comics.xkcd_marts.fact_comic
WHERE average_review < 1.0 OR average_review > 10.0;

-- Cost to views ratio is valid
SELECT Count(*) as invalid_ratio
FROM skip-comics.xkcd_marts.fact_comic
WHERE cost_to_views_ratio < 0;

-- Check for any comics in fact table that don't exist in dim table
SELECT COUNT(*) as missing
FROM skip-comics.xkcd_marts.fact_comic f
LEFT JOIN xkcd_marts.dim_comic c ON f.comic_id = c.comic_id
WHERE c.comic_id IS NULL;

--Null comic ids 
SELECT COUNT(*) AS null_comic_ids
FROM skip-comics.xkcd_marts.fact_comic
WHERE comic_id IS NULL;

-- Unique comic_ids in fact table
SELECT comic_id, COUNT(*) AS duplicate_count
FROM skip-comics.xkcd_marts.fact_comic
GROUP BY comic_id
HAVING COUNT(*) > 1;

-- Unique comic_ids in dim table
SELECT comic_id, COUNT(*) AS duplicate_count
FROM skip-comics.xkcd_marts.dim_comic
GROUP BY comic_id
HAVING COUNT(*) > 1;

--Publish date should not be null
SELECT COUNT(*) AS missing_publish_dates
FROM skip-comics.xkcd_marts.dim_date
WHERE publish_date IS NULL;
