import os
import duckdb


def run_analytics(con: duckdb.DuckDBPyConnection, out_dir: str) -> None:
    os.makedirs(os.path.join(out_dir, "user_daily"), exist_ok=True)
    os.makedirs(os.path.join(out_dir, "campaign_daily"), exist_ok=True)

    # 1. Sessionize surviving events per canonical user_id
    con.execute("""
        CREATE TEMP TABLE sessionized_events AS
        WITH ordered AS (
            SELECT
                user_id,
                ts,
                event_type,
                category,
                price_cents,
                CASE
                    WHEN LAG(ts) OVER (PARTITION BY user_id ORDER BY ts) IS NULL THEN 1
                    WHEN ts - LAG(ts) OVER (PARTITION BY user_id ORDER BY ts) > 1800000 THEN 1
                    ELSE 0
                END AS is_new_session
            FROM events
        ),
        with_session_num AS (
            SELECT
                user_id,
                ts,
                event_type,
                category,
                price_cents,
                SUM(is_new_session) OVER (
                    PARTITION BY user_id
                    ORDER BY ts
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS session_num
            FROM ordered
        )
        SELECT
            user_id,
            session_num,
            MIN(ts) AS session_start,
            CAST(to_timestamp(MIN(ts) / 1000.0) AT TIME ZONE 'UTC' AS DATE) AS day,
            COUNT(*) AS event_count,
            SUM(CASE WHEN event_type = 'purchase' THEN price_cents ELSE 0 END) AS revenue_cents
        FROM with_session_num
        GROUP BY user_id, session_num;
    """)

    # Keep event-level category revenue per (user_id, day) for top_category tie-breaking
    con.execute("""
        CREATE TEMP TABLE user_day_category_rev AS
        WITH ordered AS (
            SELECT
                user_id,
                ts,
                event_type,
                category,
                price_cents,
                CASE
                    WHEN LAG(ts) OVER (PARTITION BY user_id ORDER BY ts) IS NULL THEN 1
                    WHEN ts - LAG(ts) OVER (PARTITION BY user_id ORDER BY ts) > 1800000 THEN 1
                    ELSE 0
                END AS is_new_session
            FROM events
        ),
        with_session_num AS (
            SELECT
                user_id,
                ts,
                event_type,
                category,
                price_cents,
                SUM(is_new_session) OVER (
                    PARTITION BY user_id
                    ORDER BY ts
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS session_num
            FROM ordered
        ),
        session_days AS (
            SELECT
                user_id,
                session_num,
                CAST(to_timestamp(MIN(ts) / 1000.0) AT TIME ZONE 'UTC' AS DATE) AS day
            FROM with_session_num
            GROUP BY user_id, session_num
        )
        SELECT
            e.user_id,
            sd.day,
            e.category,
            SUM(e.price_cents) AS cat_rev
        FROM with_session_num e
        JOIN session_days sd
          ON e.user_id = sd.user_id AND e.session_num = sd.session_num
        WHERE e.event_type = 'purchase'
        GROUP BY e.user_id, sd.day, e.category;
    """)

    # 2. Write /output/user_daily/part-0.parquet
    user_daily_out = os.path.join(out_dir, "user_daily", "part-0.parquet")
    con.execute(f"""
        COPY (
            WITH ranked_cats AS (
                SELECT
                    user_id,
                    day,
                    category,
                    ROW_NUMBER() OVER (
                        PARTITION BY user_id, day
                        ORDER BY cat_rev DESC, category COLLATE "C" ASC
                    ) AS rn
                FROM user_day_category_rev
            ),
            top_cats AS (
                SELECT user_id, day, category AS top_category
                FROM ranked_cats
                WHERE rn = 1
            ),
            agg_users AS (
                SELECT
                    user_id,
                    day,
                    COUNT(*)::BIGINT AS sessions,
                    SUM(event_count)::BIGINT AS events,
                    SUM(revenue_cents)::BIGINT AS revenue_cents
                FROM sessionized_events
                GROUP BY user_id, day
            )
            SELECT
                a.user_id::VARCHAR AS user_id,
                a.day::DATE AS day,
                a.sessions::BIGINT AS sessions,
                a.events::BIGINT AS events,
                a.revenue_cents::BIGINT AS revenue_cents,
                CASE WHEN a.revenue_cents > 0 THEN t.top_category ELSE NULL END::VARCHAR AS top_category
            FROM agg_users a
            LEFT JOIN top_cats t
              ON a.user_id = t.user_id AND a.day = t.day
        ) TO '{user_daily_out}' (FORMAT parquet);
    """)

    # 3. Attribute sessions to qualifying clicks (within 7 days = 604,800,000 ms) & compute assists
    con.execute("""
        CREATE TEMP TABLE qualifying_clicks AS
        SELECT
            s.user_id,
            s.session_num,
            s.day,
            s.revenue_cents,
            c.campaign_id,
            c.click_id,
            c.ts AS click_ts
        FROM sessionized_events s
        JOIN clicks c
          ON s.user_id = c.user_id
         AND c.ts <= s.session_start
         AND s.session_start - c.ts <= 604800000;
    """)

    con.execute("""
        CREATE TEMP TABLE credited_sessions AS
        WITH ranked_clicks AS (
            SELECT
                user_id,
                session_num,
                campaign_id,
                ROW_NUMBER() OVER (
                    PARTITION BY user_id, session_num
                    ORDER BY click_ts DESC, click_id COLLATE "C" ASC
                ) AS rn
            FROM qualifying_clicks
        )
        SELECT
            s.user_id,
            s.session_num,
            s.day,
            s.revenue_cents,
            COALESCE(rc.campaign_id, 'organic') AS campaign_id
        FROM sessionized_events s
        LEFT JOIN ranked_clicks rc
          ON s.user_id = rc.user_id AND s.session_num = rc.session_num AND rc.rn = 1;
    """)

    # 4. Write /output/campaign_daily/part-0.parquet
    campaign_daily_out = os.path.join(out_dir, "campaign_daily", "part-0.parquet")
    con.execute(f"""
        COPY (
            WITH credited_agg AS (
                SELECT
                    campaign_id,
                    day,
                    COUNT(*)::BIGINT AS sessions,
                    SUM(CASE WHEN revenue_cents > 0 THEN 1 ELSE 0 END)::BIGINT AS purchasing_sessions,
                    SUM(revenue_cents)::BIGINT AS revenue_cents
                FROM credited_sessions
                GROUP BY campaign_id, day
            ),
            distinct_assists AS (
                SELECT DISTINCT
                    campaign_id,
                    day,
                    user_id,
                    session_num
                FROM qualifying_clicks
            ),
            assists_agg AS (
                SELECT
                    campaign_id,
                    day,
                    COUNT(*)::BIGINT AS assisted_sessions
                FROM distinct_assists
                GROUP BY campaign_id, day
            ),
            all_keys AS (
                SELECT campaign_id, day FROM credited_agg
                UNION
                SELECT campaign_id, day FROM assists_agg
            )
            SELECT
                k.campaign_id::VARCHAR AS campaign_id,
                CASE WHEN k.campaign_id = 'organic' THEN 'organic' ELSE cmp.channel END::VARCHAR AS channel,
                k.day::DATE AS day,
                COALESCE(cr.sessions, 0)::BIGINT AS sessions,
                COALESCE(cr.purchasing_sessions, 0)::BIGINT AS purchasing_sessions,
                COALESCE(cr.revenue_cents, 0)::BIGINT AS revenue_cents,
                COALESCE(aa.assisted_sessions, 0)::BIGINT AS assisted_sessions
            FROM all_keys k
            LEFT JOIN campaigns cmp ON k.campaign_id = cmp.campaign_id
            LEFT JOIN credited_agg cr ON k.campaign_id = cr.campaign_id AND k.day = cr.day
            LEFT JOIN assists_agg aa ON k.campaign_id = aa.campaign_id AND k.day = aa.day
        ) TO '{campaign_daily_out}' (FORMAT parquet);
    """)