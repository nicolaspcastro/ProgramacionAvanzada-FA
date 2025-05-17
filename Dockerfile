# imagen
FROM python:3.12    

# directorios
WORKDIR /app
COPY . /app

# requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# puerto 8080
EXPOSE 8080

ENV NAME World

# Run con parametros
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
