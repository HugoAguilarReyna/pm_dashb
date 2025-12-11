# Usa una imagen base ligera de Python
FROM python:3.10-slim

# Establece el directorio de trabajo dentro del contenedor
WORKDIR /app

# Copia el archivo de dependencias y las instala
# (Asegúrate de que 'requirements.txt' contenga: fastapi, uvicorn, pymongo, pandas)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el resto de tu código al contenedor
COPY . .

# Comando para iniciar la aplicación FastAPI
# (Asegúrate que el nombre del módulo sea 'app' y el objeto FastAPI se llame 'app')
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]