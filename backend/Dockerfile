# Usar una imagen base de Python estable
FROM python:3.10-slim

# 1. Establecer el directorio de trabajo dentro del contenedor
# Esto es crucial para que uvicorn encuentre main.py.
WORKDIR /app

# 2. Copiar los archivos de dependencia y seguridad
COPY requirements.txt /app/requirements.txt

# 3. Instalar dependencias
# El error anterior de 'module main not found' también desaparece con un WORKDIR correcto
RUN pip install --no-cache-dir -r /app/requirements.txt

# 4. Copiar el resto del código fuente del backend (main.py, etc.) a /app
# Dado que Render ya usa 'backend' como Root Directory, el '.' es el contenido de 'backend'.
COPY . /app

# 5. Comando de inicio (Render probablemente lo anula, pero es buena práctica)
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
