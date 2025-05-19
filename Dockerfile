# imagen
FROM python:3.12    

# directorios
WORKDIR /app
COPY . /app

# requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# puerto 8501
EXPOSE 8501

# Run con parametros
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8501"]
