import psycopg2
import os
from fastapi import FastAPI
from typing import List
from pydantic import BaseModel
import datetime

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


@app.get("/recommendations/{adv}/{modelo}")
def get_recommendations(adv: str, modelo: str):
    conn = get_connection()
    cursor = conn.cursor()

    if modelo == "top_ctr":  
        query = """
        SELECT product_id, click, impression, ctr 
        FROM top_ctr
        WHERE advertiser_id = %s AND insert_date = CURRENT_DATE
        """
    elif modelo == "top_products":  
        query = """
        SELECT product_id, views
        FROM top_products
        WHERE advertiser_id = %s AND insert_date = CURRENT_DATE
        """
    else:
        return {"error": "Modelo no válido. Debes usar 'top_ctr' o 'top_products'."}

    cursor.execute(query, (adv,))
    results = cursor.fetchall()

    recommendations = []
    for row in results:
        if modelo == "top_ctr":
            recommendations.append({
                "product_id": row[0],
                "click": row[1],
                "impression": row[2],
                "ctr": row[3]
            })
        elif modelo == "top_products":
            recommendations.append({
                "product_id": row[0],
                "views": row[1]
            })

    cursor.close()
    conn.close()
    return {"recommendations": recommendations}

@app.get("/stats/")
def get_stats():
    conn = get_connection()
    cursor = conn.cursor()

    # Cantidad de advertisers 
    query = """
    SELECT COUNT(DISTINCT advertiser_id) 
    FROM (
        SELECT advertiser_id FROM top_ctr
        UNION
        SELECT advertiser_id FROM top_products
    ) AS all_advertisers
    """
    cursor.execute(query)
    total_advertisers = cursor.fetchone()[0]

    # Variación de Recomendaciones
    query = """
    SELECT advertiser_id, COUNT(DISTINCT t.insert_date) AS var_count
    FROM (
        SELECT advertiser_id, insert_date FROM top_ctr
        UNION ALL
        SELECT advertiser_id, insert_date FROM top_products
    ) AS t
    GROUP BY advertiser_id
    ORDER BY var_count DESC
    LIMIT 10
    """
    cursor.execute(query)
    advertisers_variations = cursor.fetchall()

    # Coincidencia Modelos
    query = """
    SELECT t.advertiser_id, COUNT(DISTINCT t.model) AS model_count
    FROM (
        SELECT advertiser_id, 'ctr' AS model FROM top_ctr
        UNION ALL
        SELECT advertiser_id, 'products' AS model FROM top_products
    ) AS t
    GROUP BY t.advertiser_id
    HAVING COUNT(DISTINCT t.model) > 1
    """
    cursor.execute(query)
    model_matches = cursor.fetchall()

    cursor.close()
    conn.close()

    return {
        "total_advertisers": total_advertisers,
        "advertisers_variations": advertisers_variations,
        "model_matches": model_matches
    }



@app.get("/history/{adv}/")
def get_history(adv: str):
    conn = get_connection()
    cursor = conn.cursor()

    # Recomendaciones últimos 7 días 
    query = """
    SELECT product_id, click, impression, ctr, insert_date, NULL AS views, 'ctr' AS source
    FROM top_ctr
    WHERE advertiser_id = %s AND insert_date >= CURRENT_DATE - INTERVAL '7 days'
    UNION ALL
    SELECT product_id, NULL AS click, NULL AS impression, NULL AS ctr, insert_date, views, 'products' AS source
    FROM top_products
    WHERE advertiser_id = %s AND insert_date >= CURRENT_DATE - INTERVAL '7 days'
    """
    
    cursor.execute(query, (adv, adv))
    results = cursor.fetchall()

    history = []
    for row in results:
        
        history.append({
            "product_id": row[0],
            "click": row[1] if row[5] == 'ctr' else None,
            "impression": row[2] if row[5] == 'ctr' else None,
            "ctr": row[3] if row[5] == 'ctr' else None,
            "views": row[5] if row[5] == 'products' else None,
            "insert_date": row[4].strftime("%Y-%m-%d"),
            "source": row[5]  
        })

    cursor.close()
    conn.close()
    return {"history": history}


