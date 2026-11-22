import glob
import os
import duckdb


def run_full_sql_pipeline(
    con: duckdb.DuckDBPyConnection,
    data_dir: str,
    out_dir: str,
    win_start_ms: int,
    win_end_ms: int,
) -> None:
    os.makedirs(os.path.join(out_dir, "user_daily"), exist_ok=True)
    os.makedirs(os.path.join(out_dir, "campaign_daily"), exist_ok=True)

    click_win_start_ms = win_start_ms - 7 * 86_400_000

    con.execute("""
        CREATE TEMP MACRO is_valid_field(f) AS (
            CASE
                WHEN len(f) >= 2 AND starts_with(f, '"') AND ends_with(f, '"')
                    THEN NOT contains(substr(f, 2, len(f) - 2), '"')
                ELSE NOT contains(f, '"')
            END
        );

        CREATE TEMP MACRO unquote_strict(f) AS (
            CASE
                WHEN len(f) >= 2 AND starts_with(f, '"') AND ends_with(f, '"')
                    THEN NULLIF(substr(f, 2, len(f) - 2), '')
                ELSE NULLIF(f, '')
            END
        );

        CREATE TEMP MACRO parse_ts_ms(s) AS (
            CASE
                WHEN s IS NULL OR s = '' THEN NULL::BIGINT
                WHEN regexp_matches(s, '^[0-9]+$') THEN TRY_CAST(s AS BIGINT)
                WHEN regexp_matches(s, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z$')
                    THEN epoch_ms(try_strptime(s, '%Y-%m-%dT%H:%M:%SZ'))
                WHEN regexp_matches(s, '^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[+-][0-9]{2}:[0-9]{2}$')
                     AND TRY_CAST(substr(s, 21, 2) AS INTEGER) BETWEEN 0 AND 23
                     AND TRY_CAST(substr(s, 24, 2) AS INTEGER) BETWEEN 0 AND 59
                    THEN epoch_ms(try_strptime(substr(s, 1, 19), '%Y-%m-%dT%H:%M:%S'))
                         + (CASE WHEN substr(s, 20, 1) = '+' THEN -1 ELSE 1 END)
                           * (TRY_CAST(substr(s, 21, 2) AS BIGINT) * 3600000
                              + TRY_CAST(substr(s, 24, 2) AS BIGINT) * 60000)
                ELSE NULL::BIGINT
            END
        );
    """)

    event_files = sorted(glob.glob(os.path.join(data_dir, "events", "*.csv")))
    click_files = sorted(glob.glob(os.path.join(data_dir, "clicks", "*.csv")))

    if event_files:
        ev_list_sql = "[" + ", ".join(f"'{p.replace(chr(92), '/')}'" for p in event_files) + "]"
        con.execute(f"""
            CREATE TEMP TABLE clean_events AS
            WITH raw_files AS (
                SELECT decode(content) AS text
                FROM read_blob({ev_list_sql})
            ),
            raw_lines AS (
                SELECT unnest(string_split(text, chr(10))[2:]) AS line
                FROM raw_files
            ),
            split_cols AS (
                SELECT string_split(line, ',') AS c
                FROM raw_lines
            ),
            parsed AS (
                SELECT
                    unquote_strict(list_extract(c, 1)) AS event_id,
                    unquote_strict(list_extract(c, 2)) AS raw_user_id,
                    unquote_strict(list_extract(c, 3)) AS ts_str,
                    parse_ts_ms(unquote_strict(list_extract(c, 4))) AS ingest_ms,
                    unquote_strict(list_extract(c, 5)) AS event_type,
                    unquote_strict(list_extract(c, 6)) AS product_id,
                    unquote_strict(list_extract(c, 7)) AS price_str
                FROM split_cols
                WHERE len(c) = 7
                  AND is_valid_field(list_extract(c, 1))
                  AND is_valid_field(list_extract(c, 2))
                  AND is_valid_field(list_extract(c, 3))
                  AND is_valid_field(list_extract(c, 4))
                  AND is_valid_field(list_extract(c, 5))
                  AND is_valid_field(list_extract(c, 6))
                  AND is_valid_field(list_extract(c, 7))
            ),
            deduped AS (
                SELECT *
                FROM parsed
                WHERE event_id IS NOT NULL AND ingest_ms IS NOT NULL
                QUALIFY row_number() OVER (PARTITION BY event_id ORDER BY ingest_ms ASC) = 1
            ),
            validated AS (
                SELECT
                    COALESCE(um.canonical_user_id, d.raw_user_id) AS user_id,
                    parse_ts_ms(d.ts_str) AS ts,
                    d.ingest_ms,
                    d.event_type,
                    COALESCE(p.category, 'unknown') AS category,
                    CASE
                        WHEN d.event_type = 'purchase' THEN TRY_CAST(d.price_str AS BIGINT)
                        ELSE 0::BIGINT
                    END AS price_cents,
                    d.price_str
                FROM deduped d
                LEFT JOIN user_map um ON d.raw_user_id = um.raw_user_id
                LEFT JOIN products p ON d.product_id = p.product_id
                WHERE d.raw_user_id IS NOT NULL
                  AND d.product_id IS NOT NULL
                  AND d.event_type IN ('view', 'cart', 'purchase')
            )
            SELECT user_id, ts, event_type, category, price_cents
            FROM validated
            WHERE ts IS NOT NULL
              AND ts >= {win_start_ms} AND ts < {win_end_ms}
              AND (ingest_ms - ts) <= 86400000
              AND (
                  event_type <> 'purchase'
                  OR (
                      price_str IS NOT NULL
                      AND regexp_matches(price_str, '^[0-9]+$')
                      AND price_cents BETWEEN 1 AND 10000000
                  )
              );
        """)
    else:
        con.execute("""
            CREATE TEMP TABLE clean_events (
                user_id VARCHAR, ts BIGINT, event_type VARCHAR, category VARCHAR, price_cents BIGINT
            );
        """)

    if click_files:
        cl_list_sql = "[" + ", ".join(f"'{p.replace(chr(92), '/')}'" for p in click_files) + "]"
        con.execute(f"""
            CREATE TEMP TABLE clean_clicks AS
            WITH raw_files AS (
                SELECT decode(content) AS text
                FROM read_blob({cl_list_sql})
            ),
            raw_lines AS (
                SELECT unnest(string_split(text, chr(10))[2:]) AS line
                FROM raw_files
            ),
            split_cols AS (
                SELECT string_split(line, ',') AS c
                FROM raw_lines
            ),
            parsed AS (
                SELECT
                    unquote_strict(list_extract(c, 1)) AS click_id,
                    unquote_strict(list_extract(c, 2)) AS raw_user_id,
                    unquote_strict(list_extract(c, 3)) AS campaign_id,
                    parse_ts_ms(unquote_strict(list_extract(c, 4))) AS ts
                FROM split_cols
                WHERE len(c) = 4
                  AND is_valid_field(list_extract(c, 1))
                  AND is_valid_field(list_extract(c, 2))
                  AND is_valid_field(list_extract(c, 3))
                  AND is_valid_field(list_extract(c, 4))
            ),
            deduped AS (
                SELECT *
                FROM parsed
                WHERE click_id IS NOT NULL
                QUALIFY row_number() OVER (PARTITION BY click_id) = 1
            )
            SELECT
                d.click_id,
                COALESCE(um.canonical_user_id, d.raw_user_id) AS user_id,
                d.campaign_id,
                d.ts
            FROM deduped d
            INNER JOIN campaigns cmp ON d.campaign_id = cmp.campaign_id
            LEFT JOIN user_map um ON d.raw_user_id = um.raw_user_id
            WHERE d.raw_user_id IS NOT NULL
              AND d.ts IS NOT NULL
              AND d.ts >= {click_win_start_ms} AND d.ts < {win_end_ms};
        """)
    else:
        con.execute("""
            CREATE TEMP TABLE clean_clicks (
                click_id VARCHAR, user_id VARCHAR, campaign_id VARCHAR, ts BIGINT
            );
        """)

    con.execute("""
        CREATE TEMP TABLE event_sessions AS
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
                END AS is_new
            FROM clean_events
        )
        SELECT
            user_id,
            ts,
            event_type,
            category,
            price_cents,
            SUM(is_new) OVER (
                PARTITION BY user_id ORDER BY ts ROWS UNBOUNDED PRECEDING
            ) AS session_num
        FROM ordered;

        CREATE TEMP TABLE sessions AS
        SELECT
            user_id,
            session_num,
            MIN(ts) AS session_start,
            CAST(to_timestamp(MIN(ts) / 1000.0) AT TIME ZONE 'UTC' AS DATE) AS day,
            COUNT(*)::BIGINT AS event_count,
            SUM(CASE WHEN event_type = 'purchase' THEN price_cents ELSE 0 END)::BIGINT AS revenue_cents
        FROM event_sessions
        GROUP BY user_id, session_num;
    """)

    user_daily_out = os.path.join(out_dir, "user_daily", "part-0.parquet").replace(chr(92), "/")
    con.execute(f"""
        COPY (
            WITH cat_rev AS (
                SELECT
                    es.user_id,
                    s.day,
                    es.category,
                    SUM(es.price_cents) AS rev
                FROM event_sessions es
                JOIN sessions s ON es.user_id = s.user_id AND es.session_num = s.session_num
                WHERE es.event_type = 'purchase'
                GROUP BY es.user_id, s.day, es.category
            ),
            top_cats AS (
                SELECT user_id, day, category AS top_category
                FROM cat_rev
                QUALIFY row_number() OVER (
                    PARTITION BY user_id, day ORDER BY rev DESC, category COLLATE "C" ASC
                ) = 1
            ),
            user_agg AS (
                SELECT
                    user_id,
                    day,
                    COUNT(*)::BIGINT AS sessions,
                    SUM(event_count)::BIGINT AS events,
                    SUM(revenue_cents)::BIGINT AS revenue_cents
                FROM sessions
                GROUP BY user_id, day
            )
            SELECT
                u.user_id::VARCHAR AS user_id,
                u.day::DATE AS day,
                u.sessions,
                u.events,
                u.revenue_cents,
                CASE WHEN u.revenue_cents > 0 THEN t.top_category ELSE NULL END::VARCHAR AS top_category
            FROM user_agg u
            LEFT JOIN top_cats t ON u.user_id = t.user_id AND u.day = t.day
        ) TO '{user_daily_out}' (FORMAT parquet);
    """)

    campaign_daily_out = os.path.join(out_dir, "campaign_daily", "part-0.parquet").replace(chr(92), "/")
    con.execute(f"""
        COPY (
             WITH sorted_clicks AS (
                SELECT user_id, ts, campaign_id
                FROM clean_clicks
                QUALIFY row_number() OVER (
                    PARTITION BY user_id, ts ORDER BY click_id COLLATE "C" ASC
                ) = 1
            ),
            credited AS (
                SELECT
                    s.user_id,
                    s.session_num,
                    s.day,
                    s.revenue_cents,
                    CASE
                        WHEN c.ts IS NOT NULL AND (s.session_start - c.ts) <= 604800000
                            THEN c.campaign_id
                        ELSE 'organic'
                    END AS campaign_id
                FROM sessions s
                ASOF LEFT JOIN sorted_clicks c
                  ON s.user_id = c.user_id AND s.session_start >= c.ts
            ),
            credited_agg AS (
                SELECT
                    campaign_id,
                    day,
                    COUNT(*)::BIGINT AS sessions,
                    SUM(CASE WHEN revenue_cents > 0 THEN 1 ELSE 0 END)::BIGINT AS purchasing_sessions,
                    SUM(revenue_cents)::BIGINT AS revenue_cents
                FROM credited
                GROUP BY campaign_id, day
            ),
            distinct_assists AS (
                SELECT DISTINCT
                    c.campaign_id,
                    s.day,
                    s.user_id,
                    s.session_num
                FROM sessions s
                JOIN clean_clicks c
                  ON s.user_id = c.user_id
                 AND c.ts <= s.session_start
                 AND (s.session_start - c.ts) <= 604800000
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
