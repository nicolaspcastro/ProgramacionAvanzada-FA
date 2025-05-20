import psycopg2
import os
from fastapi import FastAPI
from datetime import date

app = FastAPI()


# Conexión db
def get_connection():
    return psycopg2.connect(
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT", 5432)
    )

# pantalla inicio


@app.get("/")
def root():
    return {"Fast_Api":"Castro-Delgado-Maximo",
            "status": "OK"}


@app.get("/recommendations/{adv}/{modelo}")
def get_recommendations(adv: str, modelo: str):
    conn = get_connection()
    cursor = conn.cursor()

    if modelo == "top_ctr":  
        query = """
        SELECT 
            product_id,
            ctr 
        FROM top_ctr
        WHERE advertiser_id = %s AND insert_date = CURRENT_DATE
        """
    elif modelo == "top_products":  
        query = """
        SELECT 
            product_id, 
            views
        FROM top_products
        WHERE advertiser_id = %s AND insert_date = CURRENT_DATE
        """
    else:
        return {"error": "Modelo no válido. Debes usar 'top_ctr' o 'top_products'."}

    cursor.execute(query, (adv,))
    results = cursor.fetchall()

    recommendations = []
    for row in results:
        recommendations.append({
            "product_id": row[0],
        })

    fecha = date.today().isoformat()

    cursor.close()
    conn.close()
    return {
        "Recomendaciones para": {
            "Advertiser": adv,
            "Fecha": fecha,
            "Modelo": modelo,
            "Productos Recomendados": recommendations
        }
    }

@app.get("/stats/")
def get_stats():
    
    
    try:
        conn = get_connection()
        cursor = conn.cursor()

        query = """
        SELECT
            -- 1. Total de advertisers
            (SELECT 
                COUNT(DISTINCT advertiser_id) 
            FROM top_ctr) AS total_advertisers,

            -- 2. Productos con CTR > 0 por advertiser 
            (
                SELECT 
                    json_agg(row_to_json(ctr_mayor_cero))
                FROM (
                    SELECT 
                        advertiser_id, 
                        COUNT(DISTINCT product_id) AS prod_ctr_positiva
                    FROM top_ctr
                    WHERE ctr > 0
                    GROUP BY advertiser_id
                ) AS ctr_mayor_cero
            ) AS prod_ctr_mayor_cero,

            -- 3. Top 10 productos con más views 
            (
                SELECT 
                    json_agg(row_to_json(top_ten))
                FROM (
                    SELECT 
                        advertiser_id, 
                        product_id, 
                        SUM(views) AS total_views
                    FROM top_products
                    GROUP BY advertiser_id, product_id
                    ORDER BY total_views DESC
                    LIMIT 10
                ) AS top_ten
            ) AS top_10_adv_productos,

            -- 4. Promedio y máximo CTR por día 
            (
                SELECT 
                    json_agg(row_to_json(mean_max_ctr))
                FROM (
                    SELECT 
                        insert_date,
                        ROUND(AVG(ctr)::numeric, 4) AS promedio_ctr,
                        ROUND(MAX(ctr)::numeric, 4) AS max_ctr
                    FROM top_ctr
                    GROUP BY insert_date
                    ORDER BY insert_date
                ) AS mean_max_ctr
            ) AS max_mean_ctr_por_dia;
        """

        cursor.execute(query)
        row = cursor.fetchone()

        result = {
            "Total Advertisers": row[0],
            "Q productos con CTR>0": row[1] if row[1] else [],
            "Top Ten Adv+prod": row[2] if row[2] else [],
            "Mean and Max CTR": row[3] if row[3] else []
        }

        return result
    except Exception as e:
        return {"error": str(e)}

    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()



@app.get("/history/{adv}/")
def get_history(adv: str):
    
    conn = get_connection()
    cursor = conn.cursor()

    query = """
    SELECT 
        -- 1. Datos de top_ctr (últimos 7 días)
        (
            SELECT 
                json_agg(row_to_json(l7_top_ctr))
            FROM (
                SELECT 
                    insert_date, 
                    product_id
                FROM top_ctr
                WHERE advertiser_id = %s 
                    AND insert_date >= CURRENT_DATE - INTERVAL '6 days'
                ORDER BY insert_date
            ) AS l7_top_ctr
        ) AS top_ctr,

        -- 2. Datos de top_products (últimos 7 días)
        (
            SELECT 
                json_agg(row_to_json(l7_top_products))
            FROM (
                SELECT 
                    insert_date, 
                    product_id
                FROM top_products
                WHERE advertiser_id = %s 
                    AND insert_date >= CURRENT_DATE - INTERVAL '6 days'
                ORDER BY insert_date
            ) AS l7_top_products
        ) AS top_products
    """

    cursor.execute(query, (adv, adv))
    row = cursor.fetchone()

    cursor.close()
    conn.close()

    return {
        "Recomendaciones para": {
            "Advertiser": adv,
            "top_ctr": row[0] if row[0] is not None else [],
            "top_products": row[1] if row[1] is not None else []
        }
    }



@app.get("/test/{metrica}/")
def test(metrica: str):
    conn = get_connection()
    cursor = conn.cursor()

    if metrica == "adv":
        query = """
        SELECT advertiser_id, COUNT(DISTINCT(product_id))
        FROM top_ctr
        WHERE insert_date = CURRENT_DATE
        GROUP BY advertiser_id
        """
    elif metrica == "product":
        query = """
        SELECT product_id, COUNT(DISTINCT(product_id))
        FROM top_ctr
        WHERE insert_date = CURRENT_DATE
        GROUP BY product_id
        """
    else:
        return {"error": "Métrica no válida. Usa 'adv' o 'product'."}

    cursor.execute(query)
    results = cursor.fetchall()

    test = []
    for row in results:
        test.append({
            "id": row[0],
            "cantidad": row[1]
        })

    cursor.close()
    conn.close()
    return {"test": test}
