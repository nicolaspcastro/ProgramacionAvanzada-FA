import psycopg2
import os
from fastapi import FastAPI
from typing import List
from pydantic import BaseModel
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
    conn = get_connection()
    cursor = conn.cursor()

    # 1. Cantidad de advertisers distintos
    query = """
    SELECT 
        COUNT(DISTINCT advertiser_id) AS total_advertisers
    FROM top_ctr;
    """
    cursor.execute(query)
    total_advertisers = cursor.fetchone()[0]

    # 2. Cantidad de productos con CTR>0, por advertiser
    query = """
    SELECT 
        advertiser_id, 
        COUNT(DISTINCT product_id) AS prod_ctr_positiva
    FROM top_ctr
    WHERE ctr > 0
    GROUP BY advertiser_id;
    """
    cursor.execute(query)
    ctr_over_zero = cursor.fetchall()

    ctr_mayor_cero = []
    for row in ctr_over_zero:
        ctr_mayor_cero.append({
            "advertiser_id": row[0],
            "cantidad de prod con CTR >0": row[1]
        })

    # 3. Top 10 pares [advertiser, producto] con más view
    query = """
    SELECT 
        advertiser_id, 
        product_id, 
        SUM(views) AS total_views
    FROM top_products
    GROUP BY advertiser_id, product_id
    ORDER BY total_views DESC
    LIMIT 10;
    """
    cursor.execute(query)
    top_ten = cursor.fetchall()

    cursor.close()
    conn.close()

    ranking = []
    for row in top_ten:
        ranking.append({
            "advertiser_id": row[0],
            "product_id": row[1],
            "views": row[2]
        })

    # 4. Maximo y Media de CTR por día
    query = """
    SELECT 
        insert_date,
        ROUND(AVG(ctr), 4) AS promedio_ctr,
        ROUND(MAX(ctr), 4) AS max_ctr
    FROM top_ctr
    GROUP BY insert_date
    ORDER BY insert_date;
    """
    cursor.execute(query)
    max_mean_ctr = cursor.fetchall()

    cursor.close()
    conn.close()

    max_media = []
    for row in max_mean_ctr:
        max_media.append({
            "fecha": row[0],
            "promedio CTR": row[1],
            "maximo CTR": row[2]
        })


    return {
        "Total Advertisers": total_advertisers,
        "Q productos con CTR>0": ctr_mayor_cero,
        "Top Ten Adv+prod": ranking,
        "Mean and Max CTR": max_media
    }



@app.get("/history/{adv}/")
def get_history(adv: str):
    conn = get_connection()
    cursor = conn.cursor()

    # 1. Recomendaciones top_ctr
    query = """
    SELECT 
        insert_date,
        product_id,
        ctr 
    FROM top_ctr
    WHERE advertiser_id = %s 
        AND insert_date >= CURRENT_DATE - INTERVAL '6 days'
    ORDER BY insert_date
    """
    cursor.execute(query, (adv,))
    results_ctr = cursor.fetchall()

    recommendations_ctr = []
    for row in results_ctr:
        recommendations_ctr.append({
            "fecha": row[0].isoformat(),
            "product_id": row[1]
        })
    
    cursor.close()
    conn.close()

    # 2. Recomendaciones top_products

    query = """
    SELECT 
        insert_date,
        product_id,
        views
    FROM top_products
    WHERE advertiser_id = %s 
        AND insert_date >= CURRENT_DATE - INTERVAL '6 days'
    ORDER BY insert_date
    """

    cursor.execute(query, (adv,))
    results_products = cursor.fetchall()

    recommendations_products = []
    for row in results_products:
        recommendations_products.append({
            "fecha": row[0].isoformat(),
            "product_id": row[1]
        })

    cursor.close()
    conn.close()

    return {
        "Recomendaciones para": {
            "Advertiser": adv,
            "top_ctr": recommendations_ctr,
            "top_products": recommendations_products
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
