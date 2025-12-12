# Usar una imagen base de Python estable
FROM python:3.10-slim

# 1. Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# 2. Copiar los archivos de dependencia y seguridad
# (Asumiendo que tienes un requirements.txt en backend/)
COPY requirements.txt .

# 3. Instalar dependencias
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copiar el resto del código fuente del backend a /app
# El contexto de Render es `backend/`, por lo que el `.` es el contenido de esa carpeta.
COPY . .

# 5. Exponer el puerto de Uvicorn (opcional, pero buena práctica)
EXPOSE 8000

# NOTA: Render está ignorando el CMD de aquí porque lo estableciste
# en la configuración de Render, lo cual es correcto para usar $PORT.
# CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
