

# Usar una imagen base de Python estable
    FROM python:3.10-slim

    # Establecer el directorio de trabajo dentro del contenedor
    WORKDIR /app

    # 1. Copiar los archivos de dependencia. 
    # Dado que el CONTEXTO es 'backend/', requirements.txt está directamente en la raíz del contexto.
    # COPY requirements.txt /app/requirements.txt
    COPY backend/requirements.txt /app/requirements.txt

    # 2. Instalar dependencias
    RUN pip install --no-cache-dir -r /app/requirements.txt

    # 3. Copiar el resto del código fuente del backend. 
    # Esto copia el contenido del CONTEXTO (que es la carpeta 'backend') a /app/
    COPY . /app

    # 4. Comando de inicio
    CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]

