import os
import json
from fastapi import FastAPI, Query, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
from bson import json_util
import io
import pandas as pd
import numpy as np
from typing import Optional

app = FastAPI(title="Dashboard Tesina API")

# --- Configuración CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Conexión MongoDB Corregida ---
# Buscamos MONGO_ATLAS_URI, MONGO_URI, o caemos al valor local.
mongo_uri = os.getenv("MONGO_ATLAS_URI") or os.getenv("MONGO_URI")
# Buscamos DB_NAME o usamos el valor por defecto si no está configurado.
db_name = os.getenv("DB_NAME") or "project_dashboard"

# Fallback para desarrollo local
if not mongo_uri:
    mongo_uri = "mongodb://localhost:27017/"

# Inicialización de la conexión
client = None
db = None
try:
    client = MongoClient(mongo_uri)
    db = client[db_name] 
    
    # Comprobación de conexión (opcional, pero buena práctica)
    client.admin.command('ping')
    print(f"Conexión exitosa a MongoDB. Base de Datos: {db_name}")

except Exception as e:
    print(f"--- ERROR CRÍTICO DE CONEXIÓN A MONGO ---")
    print(f"URI usada: {mongo_uri}. DB: {db_name}")
    print(f"Error: {e}")
    # Lanzamos una excepción para detener la aplicación si la DB es vital
    raise HTTPException(status_code=500, detail=f"Fallo al conectar con la base de datos: {e}")


# --- Funciones Auxiliares ---
def parse_json(data):
    return json.loads(json_util.dumps(data))

def to_upper(s):
    return s.upper() if isinstance(s, str) else s

def safe_date_parse(date_value):
    """
    Convierte un valor a datetime naive (sin tz).
    Soporta múltiples formatos: DD/MM/YYYY, DD/MM/YYYY HH:MM:SS, YYYY-MM-DD, YYYY-MM-DD HH:MM:SS.
    """
    if not date_value or str(date_value).lower() in ['nan', 'nat', 'none']:
        return None
    
    date_str = str(date_value).split('.')[0] # Remueve milisegundos si vienen de numpy
    
    formats = [
        '%d/%m/%Y', '%d/%m/%Y %H:%M:%S', 
        '%Y-%m-%d', '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M:%S%z', # con timezone (lo convertiremos a naive)
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            # Asegurar que sea naive (sin zona horaria)
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        except ValueError:
            continue
            
    try:
        # Intenta parsear como fecha ISO que pandas puede producir
        dt = pd.to_datetime(date_value, errors='coerce')
        if pd.notna(dt):
            # Convertir a datetime de Python y hacerlo naive
            if dt.tzinfo is not None:
                dt = dt.tz_convert(None) # Remueve timezone si existe
            return dt.to_pydatetime()
    except Exception:
        pass

    print(f"Advertencia: No se pudo parsear la fecha/hora: {date_value}")
    return None

def is_date_valid(date_value):
    """Verifica si la fecha es un objeto datetime."""
    return isinstance(date_value, datetime)


# =======================================================
# 📎 CARGA DE DATOS (CSV)
# =======================================================
@app.post("/api/ingest-csv")
async def ingest_csv_data(file: UploadFile = File(...)):
    if not db:
        raise HTTPException(status_code=503, detail="Servicio de base de datos no disponible.")
        
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Solo se permiten archivos CSV.")

    try:
        # Leer contenido del archivo
        content = await file.read()
        csv_file = io.StringIO(content.decode('utf-8'))
        
        # Leer CSV en DataFrame
        df = pd.read_csv(csv_file)
        
        # 1. Limpieza y estandarización de encabezados
        df.columns = df.columns.str.strip().str.replace(' ', '_').str.lower()
        
        # 2. Renombrar columnas clave si es necesario (Asegúrate que estas columnas coincidan con tu CSV)
        df = df.rename(columns={
            'task_id': 'id', # Debería ser la clave primaria
            'project_name': 'project',
            'status': 'status',
            'due_date': 'end', 
            'start_date': 'start',
            'assigned_to': 'user',
            'estimated_effort_hrs': 'effort_hrs',
            'description': 'text' # Para el Gantt
        })
        
        # 3. Conversión de fechas y estandarización de status
        
        # Asegurar que las columnas clave existan antes de procesar
        required_cols = ['id', 'project', 'text', 'status', 'start', 'end', 'user']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise HTTPException(status_code=400, detail=f"Faltan columnas requeridas en el CSV: {', '.join(missing_cols)}")


        # Intentar convertir las fechas
        df['start'] = df['start'].apply(safe_date_parse)
        df['end'] = df['end'].apply(safe_date_parse)
        
        # Filtrar filas donde las fechas no pudieron ser parseadas (si no son válidas, se ignoran o corrigen)
        df.dropna(subset=['start', 'end'], inplace=True) 
        
        # Estandarizar status (mayúsculas)
        df['status'] = df['status'].apply(to_upper)
        
        # 4. Preparar para MongoDB
        # Convertir NaN a None (importante para json.loads)
        df = df.replace({np.nan: None}) 
        
        # Convertir DataFrame a lista de diccionarios
        data_to_insert = df.to_dict('records')
        
        if not data_to_insert:
             raise HTTPException(status_code=400, detail="El archivo CSV no contiene datos válidos después de la limpieza.")

        # 5. Insertar o actualizar en MongoDB
        collection = db["tasks"]
        
        # Usamos replace_one para manejar updates si el 'id' ya existe (upsert)
        updates = 0
        inserts = 0
        
        for record in data_to_insert:
            # Creamos un filtro por el ID de la tarea
            filter_query = {'id': record['id']}
            
            # El campo _id se maneja automáticamente por MongoDB; no lo incluimos en el update si existe
            record.pop('_id', None) 
            
            result = collection.replace_one(
                filter_query,
                record,
                upsert=True # Si no existe, lo inserta
            )
            
            if result.modified_count == 1 or result.matched_count == 1 and not result.upserted_id:
                updates += 1
            elif result.upserted_id:
                inserts += 1

        # Actualizar el registro de la última actualización
        db["metadata"].replace_one(
            {"key": "last_update"}, 
            {"key": "last_update", "timestamp": datetime.now(timezone.utc)}, 
            upsert=True
        )

        return {"message": "Datos de tareas actualizados con éxito.", 
                "total_records": len(data_to_insert), 
                "inserted": inserts,
                "updated": updates,
                "db_name": db_name,
                "collection": "tasks"}

    except HTTPException:
        # Re-lanza las excepciones HTTPException para que FastAPI las maneje
        raise
    except Exception as e:
        print(f"Error general en ingest_csv_data: {e}")
        raise HTTPException(status_code=500, detail=f"Error interno del servidor durante la ingesta: {e}")

# =======================================================
# 📊 OBTENCIÓN DE DATOS (GANTT, TABLA)
# =======================================================

@app.get("/api/tasks/all")
async def get_all_tasks():
    if not db:
        raise HTTPException(status_code=503, detail="Servicio de base de datos no disponible.")
        
    try:
        tasks = list(db["tasks"].find({}))
        
        if not tasks:
            return parse_json([]) # Retorna lista vacía si no hay tareas
        
        # Devolvemos un JSON parseado
        return parse_json(tasks)
        
    except Exception as e:
        print(f"Error en get_all_tasks: {e}")
        raise HTTPException(status_code=500, detail=f"Error al obtener las tareas: {e}")

@app.get("/api/tasks/upcoming")
async def get_upcoming_tasks(days: Optional[int] = Query(30, description="Número de días a futuro para filtrar.")):
    if not db:
        raise HTTPException(status_code=503, detail="Servicio de base de datos no disponible.")

    try:
        now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        future_date = now + timedelta(days=days)
        
        # Filtramos por tareas que terminan entre hoy y la fecha futura, y que no estén completadas
        query = {
            "end": {"$gte": now, "$lte": future_date},
            "status": {"$ne": to_upper("COMPLETED")} 
        }
        
        tasks = list(db["tasks"].find(query).sort("end", 1)) # Ordenar por fecha de fin ascendente
        
        return parse_json(tasks)
        
    except Exception as e:
        print(f"Error en get_upcoming_tasks: {e}")
        raise HTTPException(status_code=500, detail=f"Error al obtener las tareas futuras: {e}")

# =======================================================
# 📈 MÉTRICAS (SCOREBOARD)
# =======================================================
@app.get("/api/metrics")
async def get_metrics():
    if not db:
        return parse_json({"total_tasks": 0, "completed_tasks": 0, "completion_rate": 0, "avg_completion_time": 0})
        
    try:
        collection = db["tasks"]
        
        # 1. Total de Tareas
        total_tasks = await collection.count_documents({})
        
        # 2. Tareas Completadas
        completed_tasks = await collection.count_documents({"status": to_upper("COMPLETED")})
        
        # 3. Tasa de Finalización
        completion_rate = round((completed_tasks / total_tasks) * 100, 1) if total_tasks > 0 else 0
        
        # 4. Tiempo promedio de finalización (Lógica Placeholder, ya que depende de campos 'completed_at')
        # Si tienes un campo 'completed_at', usa lógica real.
        # Por ahora, se mantiene el valor fijo para evitar fallos si no se puede calcular.
        avg_completion_time = 5.2 
        
        return parse_json({
            "total_tasks": total_tasks, "completed_tasks": completed_tasks,
            "completion_rate": completion_rate, "avg_completion_time": avg_completion_time
        })
    except Exception as e:
        print(f"Error en get_metrics: {e}")
        return parse_json({"total_tasks": 0, "completed_tasks": 0, "completion_rate": 0, "avg_completion_time": 0})

@app.get("/api/metrics/summary")
async def get_metrics_summary():
    return await get_metrics()

# =======================================================
# 🩺 HEALTH, DAILY, FAVICON
# =======================================================
@app.get("/health")
async def health_check():
    return {"status": "healthy", "timestamp": datetime.now(timezone.utc).isoformat()}

@app.get("/api/tasks/daily")
async def get_daily_tasks():
    # Asume que el frontend usará el endpoint /api/tasks/upcoming para el gráfico diario
    return await get_upcoming_tasks()

@app.get("/favicon.ico")
async def favicon():
    raise HTTPException(status_code=404, detail="No favicon configured")

# =======================================================
# ▶️ MAIN (No usado en Uvicorn, pero útil si lo corres directamente)
# =======================================================
if __name__ == "__main__":
    import uvicorn
    # Usa un puerto por defecto para pruebas locales
    uvicorn.run(app, host="0.0.0.0", port=8000)
