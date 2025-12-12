# main.py - BACKEND COMPLETO Y CORREGIDO PARA DASHBOARD TESINA (AJUSTES DE INGESTA)

import os
import json
from fastapi import FastAPI, Query, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
from bson import json_util, ObjectId
import io
import pandas as pd
import numpy as np
from typing import Optional, List
import logging
import urllib.parse

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Dashboard Tesina API",
    description="API para el dashboard de gestión de proyectos",
    version="2.0.0"
)

# --- Configuración CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permitir todos los orígenes
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =======================================================
# CONEXIÓN MONGODB ATLAS - CONFIGURACIÓN CORREGIDA
# =======================================================
# Tu URI de MongoDB Atlas
MONGO_ATLAS_URI = "mongodb+srv://aguilarhugo55_db_user:c5mfG11QT68ib4my@clusteract1.kpdhd5e.mongodb.net/?appName=ClusterAct1"
DB_NAME = "project_dashboard"

# Inicialización de la conexión
client = None
db = None

try:
    # CONEXIÓN DIRECTA CON TIMEOUTS AJUSTADOS
    client = MongoClient(
        MONGO_ATLAS_URI,
        serverSelectionTimeoutMS=10000,  # 10 segundos para seleccionar servidor
        connectTimeoutMS=10000,         # 10 segundos para conectar
        socketTimeoutMS=30000,          # 30 segundos para operaciones
        retryWrites=True,
        w="majority"
    )
    
    # Verificar conexión
    client.admin.command('ping')
    db = client[DB_NAME]
    
    logger.info(f"✅ Conexión exitosa a MongoDB Atlas")
    logger.info(f"📁 Base de datos: {DB_NAME}")
    
except Exception as e:
    logger.error(f"❌ Error de conexión a MongoDB Atlas: {e}")
    logger.warning("🚨 Continuando en modo sin base de datos para pruebas")
    client = None
    db = None

# --- Funciones Auxiliares ---
def parse_json(data):
    """Convierte datos MongoDB a JSON serializable."""
    return json.loads(json_util.dumps(data))

def to_upper(s):
    """Convierte string a mayúsculas."""
    return s.upper() if isinstance(s, str) else s

def safe_date_parse(date_value):
    """
    Convierte un valor a datetime naive (sin tz).
    Soporta múltiples formatos.
    """
    if not date_value or str(date_value).lower() in ['nan', 'nat', 'none', 'null']:
        return None
    
    date_str = str(date_value).split('.')[0]  # Remueve milisegundos
    
    formats = [
        '%d/%m/%Y', '%d/%m/%Y %H:%M:%S',
        '%Y-%m-%d', '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M:%S%z',
        '%Y-%m-%dT%H:%M:%S', '%Y-%m-%dT%H:%M:%SZ',
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(date_str, fmt)
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt
        except ValueError:
            continue
            
    try:
        dt = pd.to_datetime(date_value, errors='coerce')
        if pd.notna(dt):
            if dt.tzinfo is not None:
                dt = dt.tz_convert(None)
            return dt.to_pydatetime()
    except Exception:
        pass

    logger.warning(f"No se pudo parsear la fecha: {date_value}")
    return None

def format_task_for_response(task):
    """Formatea una tarea para la respuesta API."""
    if not task:
        return None
    
    # Convertir ObjectId a string
    if '_id' in task and isinstance(task['_id'], ObjectId):
        task['_id'] = str(task['_id'])
    
    # Asegurar que las fechas sean strings ISO
    date_fields = ['start', 'end', 'due_date', 'start_date', 'end_date', 'created_at', 'updated_at']
    for field in date_fields:
        if field in task and isinstance(task[field], datetime):
            task[field] = task[field].isoformat()
    
    return task

def is_db_available():
    """Verifica si la base de datos está disponible."""
    return client is not None and db is not None

# =======================================================
# ENDPOINTS DE INGESTA
# =======================================================
@app.post("/api/ingest-csv")
async def ingest_csv_data(file: UploadFile = File(...)):
    """Endpoint original para ingestión de CSV."""
    if not is_db_available():
        raise HTTPException(status_code=503, detail="Servicio de base de datos no disponible.")
        
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Solo se permiten archivos CSV.")

    try:
        content = await file.read()
        csv_file = io.StringIO(content.decode('utf-8'))
        df = pd.read_csv(csv_file)
        
        logger.info(f"Archivo CSV cargado: {file.filename}, {len(df)} filas, {len(df.columns)} columnas")
        
        # Limpieza y estandarización de encabezados
        # (Se mantiene la línea para estandarizar a minúsculas y sin espacios)
        df.columns = df.columns.str.strip().str.replace(' ', '_').str.lower()
        
        # Renombrar columnas clave (AJUSTE AQUÍ: se añaden más nombres comunes)
        column_mapping = {
            'task_id': 'id',
            'project_name': 'project',
            'project': 'project',
            'status': 'status',
            'due_date': 'end',
            'due': 'end',
            'start_date': 'start',
            'start': 'start',
            'assigned_to': 'user',
            'assigned_user_id': 'user',
            'user': 'user',
            'assigned': 'user',
            'estimated_effort_hrs': 'effort_hrs',
            # -- MAPEOS CORREGIDOS/AMPLIADOS PARA EL CAMPO TEXT --
            'description': 'text',
            'name': 'text',
            'title': 'text',
            'task_name': 'text',
            'task': 'text',
            'nombre': 'text',  # <--- AÑADIDO
            'tarea': 'text',   # <--- AÑADIDO
            'duration': 'duration'
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df.rename(columns={old_col: new_col}, inplace=True)
        
        # Verificar columnas requeridas
        # Si la columna 'text' ya está mapeada o existía, no debería faltar aquí.
        required_cols = ['text', 'status', 'start', 'end']
        missing_cols = [col for col in required_cols if col not in df.columns]
        
        # CORRECCIÓN DE LA LÓGICA DE ERROR:
        # Esto lanzará el error 400 si falta alguna de las 4 columnas (text, status, start, end).
        if missing_cols:
            raise HTTPException(
                status_code=400,
                detail=f"Faltan columnas requeridas: {', '.join(missing_cols)}"
            )
        
        # Generar IDs si no existen
        if 'id' not in df.columns:
            df['id'] = [f"TASK_{i+1:04d}" for i in range(len(df))]
        
        if 'project' not in df.columns:
            df['project'] = 'Proyecto General'
        
        if 'user' not in df.columns:
            df['user'] = 'N/A'
        
        # Procesar fechas
        df['start'] = df['start'].apply(safe_date_parse)
        df['end'] = df['end'].apply(safe_date_parse)
        
        # Filtrar filas sin fechas válidas
        initial_count = len(df)
        df = df[df['start'].notna() & df['end'].notna()].copy()
        filtered_count = initial_count - len(df)
        
        if filtered_count > 0:
            logger.warning(f"Se filtraron {filtered_count} filas sin fechas válidas")
        
        if len(df) == 0:
            raise HTTPException(status_code=400, detail="No hay filas con fechas válidas después del filtrado")
        
        # Estandarizar status
        df['status'] = df['status'].apply(lambda x: to_upper(str(x)) if pd.notna(x) else 'TO_DO')
        
        # Añadir campos adicionales
        df['created_at'] = datetime.now()
        df['updated_at'] = datetime.now()
        
        # Preparar para MongoDB
        df = df.replace({np.nan: None})
        data_to_insert = df.to_dict('records')
        
        logger.info(f"Preparadas {len(data_to_insert)} tareas para insertar")
        
        # Insertar/actualizar en MongoDB
        collection = db["tasks"]
        updates = 0
        inserts = 0
        
        for record in data_to_insert:
            try:
                filter_query = {'id': record['id']}
                record.pop('_id', None)
                
                result = collection.replace_one(filter_query, record, upsert=True)
                
                if result.modified_count > 0:
                    updates += 1
                elif result.upserted_id:
                    inserts += 1
            except Exception as e:
                logger.error(f"Error procesando registro {record.get('id')}: {e}")
        
        # Registrar última actualización
        db["metadata"].replace_one(
            {"key": "last_update"},
            {"key": "last_update", "timestamp": datetime.now(timezone.utc), "ingestion": True},
            upsert=True
        )
        
        return {
            "status": "success",
            "message": "Datos actualizados exitosamente",
            "total_records": len(data_to_insert),
            "inserted": inserts,
            "updated": updates,
            "database": DB_NAME,
            "timestamp": datetime.now().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error en ingest_csv_data: {e}")
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")

@app.post("/api/ingest/tasks")
async def ingest_tasks(file: UploadFile = File(...)):
    """Endpoint alternativo para compatibilidad con frontend."""
    return await ingest_csv_data(file)

# =======================================================
# ENDPOINTS DE TAREAS
# =======================================================
@app.get("/api/tasks/all")
async def get_all_tasks():
    """Obtiene todas las tareas."""
    if not is_db_available():
        raise HTTPException(status_code=503, detail="Servicio no disponible. MongoDB no conectado.")
    
    try:
        tasks = list(db["tasks"].find({}).sort("start", 1))
        formatted_tasks = [format_task_for_response(task) for task in tasks]
        
        return parse_json(formatted_tasks)
    except Exception as e:
        logger.error(f"Error en get_all_tasks: {e}")
        raise HTTPException(status_code=500, detail=f"Error al obtener tareas: {str(e)}")

@app.get("/api/tasks/overdue")
async def get_overdue_tasks():
    """Obtiene tareas vencidas no completadas."""
    if not is_db_available():
        # Modo demo si no hay DB
        return parse_json([
            {
                "id": "TASK_001",
                "name": "Tarea vencida de ejemplo",
                "title": "Tarea vencida de ejemplo",
                "assigned_user_id": "USER_1",
                "assigned_to": "USER_1",
                "user": "USER_1",
                "status": "TO_DO",
                "days_overdue": 5,
                "text": "Esta tarea está vencida"
            }
        ])
    
    try:
        now = datetime.now()
        query = {
            "end": {"$lt": now},
            "status": {"$ne": "COMPLETED"}
        }
        
        tasks = list(db["tasks"].find(query).sort("end", 1))
        
        # Calcular días de retraso
        for task in tasks:
            if task.get("end"):
                if isinstance(task["end"], str):
                    task["end"] = safe_date_parse(task["end"])
                
                if isinstance(task["end"], datetime):
                    days_overdue = (now - task["end"]).days
                    task["days_overdue"] = max(days_overdue, 1)
                else:
                    task["days_overdue"] = 1
            else:
                task["days_overdue"] = 1
        
        formatted_tasks = [format_task_for_response(task) for task in tasks]
        
        return parse_json(formatted_tasks)
    except Exception as e:
        logger.error(f"Error en get_overdue_tasks: {e}")
        return parse_json([])

@app.get("/api/tasks/upcoming")
async def get_upcoming_tasks(days: Optional[int] = Query(30, ge=1)):
    """Obtiene tareas próximas a vencer."""
    if not is_db_available():
        return parse_json([])
    
    try:
        now = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        future_date = now + timedelta(days=days)
        
        query = {
            "end": {"$gte": now, "$lte": future_date},
            "status": {"$ne": "COMPLETED"}
        }
        
        tasks = list(db["tasks"].find(query).sort("end", 1))
        formatted_tasks = [format_task_for_response(task) for task in tasks]
        
        return parse_json(formatted_tasks)
    except Exception as e:
        logger.error(f"Error en get_upcoming_tasks: {e}")
        return parse_json([])

@app.get("/api/tasks/daily")
async def get_daily_tasks():
    """Obtiene tareas para el día actual."""
    if not is_db_available():
        return parse_json([])
    
    try:
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        tomorrow = today + timedelta(days=1)
        
        query = {
            "end": {"$gte": today, "$lt": tomorrow},
            "status": {"$ne": "COMPLETED"}
        }
        
        tasks = list(db["tasks"].find(query).sort("end", 1))
        formatted_tasks = [format_task_for_response(task) for task in tasks]
        
        return parse_json(formatted_tasks)
    except Exception as e:
        logger.error(f"Error en get_daily_tasks: {e}")
        return parse_json([])

# =======================================================
# ENDPOINT GANTT
# =======================================================
@app.get("/api/tasks/gantt")
async def get_gantt_data(
    status: Optional[str] = Query(None),
    user_id: Optional[str] = Query(None),
    project: Optional[str] = Query(None)
):
    """Obtiene datos para el diagrama de Gantt."""
    if not is_db_available():
        # Modo demo con datos de ejemplo
        return {
            "data": [],
            "filters": {
                "projects": ["Proyecto 1", "Proyecto 2"],
                "users": ["USER_1", "USER_2", "N/A"],
                "statuses": ["TO_DO", "IN_PROGRESS", "COMPLETED", "BLOCKED"]
            }
        }
    
    try:
        # Construir query
        query = {}
        if status:
            query["status"] = to_upper(status)
        if user_id:
            if user_id == "N/A":
                query["user"] = {"$in": ["N/A", None, ""]}
            else:
                query["user"] = user_id
        if project:
            query["project"] = {"$regex": f".*{project}.*", "$options": "i"}
        
        tasks = list(db["tasks"].find(query))
        
        # Formatear para Gantt
        gantt_tasks = []
        for task in tasks:
            # Obtener fechas
            start_date = task.get("start")
            end_date = task.get("end")
            
            # Parsear fechas si son strings
            if isinstance(start_date, str):
                start_date = safe_date_parse(start_date)
            if isinstance(end_date, str):
                end_date = safe_date_parse(end_date)
            
            if not start_date or not end_date:
                continue
            
            # Calcular duración
            duration_days = max((end_date - start_date).days, 1)
            
            # Obtener usuario
            user = task.get("user", "N/A")
            if user is None or user == "":
                user = "N/A"
            
            gantt_task = {
                "_id": str(task.get("_id", "")),
                "id": task.get("id", f"TASK_{len(gantt_tasks)+1:04d}"),
                "name": task.get("text", task.get("name", "Tarea sin nombre")),
                "title": task.get("text", task.get("name", "Tarea sin nombre")),
                "start_date": start_date.isoformat() if isinstance(start_date, datetime) else str(start_date),
                "end_date": end_date.isoformat() if isinstance(end_date, datetime) else str(end_date),
                "due_date": end_date.isoformat() if isinstance(end_date, datetime) else str(end_date),
                "assigned_user_id": user,
                "assigned_to": user,
                "status": task.get("status", "TO_DO"),
                "user": user,
                "project": task.get("project", "Sin Proyecto"),
                "duration_days": duration_days,
                "progress": task.get("progress", 0),
                "text": task.get("text", task.get("name", "Tarea sin nombre"))
            }
            gantt_tasks.append(gantt_task)
        
        # Obtener filtros disponibles
        try:
            projects = [p for p in db["tasks"].distinct("project") if p and str(p).strip()]
            users = [u for u in db["tasks"].distinct("user") if u and str(u).strip()]
            statuses = [s for s in db["tasks"].distinct("status") if s and str(s).strip()]
        except:
            projects = []
            users = []
            statuses = []
        
        # Añadir "N/A" a usuarios si no está
        if "N/A" not in users:
            users.append("N/A")
        
        return {
            "data": gantt_tasks,
            "filters": {
                "projects": projects or ["Proyecto 1", "Proyecto 2"],
                "users": users or ["USER_1", "USER_2", "N/A"],
                "statuses": statuses or ["TO_DO", "IN_PROGRESS", "COMPLETED", "BLOCKED"]
            }
        }
        
    except Exception as e:
        logger.error(f"Error en get_gantt_data: {e}")
        return {
            "data": [],
            "filters": {
                "projects": [],
                "users": [],
                "statuses": []
            }
        }

# =======================================================
# ENDPOINT ESTADO DEL PROYECTO
# =======================================================
@app.get("/api/project/status")
async def get_project_status():
    """Obtiene el estado de todos los proyectos."""
    if not is_db_available():
        # Modo demo
        return {
            "projects": [
                {
                    "_id": "Proyecto 1",
                    "statuses": [
                        {"status": "TO_DO", "count": 3},
                        {"status": "IN_PROGRESS", "count": 2},
                        {"status": "COMPLETED", "count": 5}
                    ],
                    "total_tasks": 10,
                    "completed_tasks": 5,
                    "completion_rate": 50.0
                }
            ],
            "summary": {
                "total_projects": 1,
                "total_tasks": 10,
                "total_completed": 5,
                "overall_completion_rate": 50.0
            }
        }
    
    try:
        collection = db["tasks"]
        
        # Pipeline para agrupar por proyecto y estado
        pipeline = [
            {
                "$match": {
                    "project": {"$exists": True, "$ne": None, "$ne": ""}
                }
            },
            {
                "$group": {
                    "_id": {
                        "project": "$project",
                        "status": "$status"
                    },
                    "count": {"$sum": 1}
                }
            },
            {
                "$group": {
                    "_id": "$_id.project",
                    "statuses": {
                        "$push": {
                            "status": "$_id.status",
                            "count": "$count"
                        }
                    },
                    "total_tasks": {"$sum": "$count"}
                }
            },
            {
                "$project": {
                    "project": "$_id",
                    "statuses": 1,
                    "total_tasks": 1,
                    "completed": {
                        "$filter": {
                            "input": "$statuses",
                            "as": "status",
                            "cond": {"$eq": ["$$status.status", "COMPLETED"]}
                        }
                    }
                }
            },
            {
                "$project": {
                    "project": 1,
                    "total_tasks": 1,
                    "completed_tasks": {"$arrayElemAt": ["$completed.count", 0]},
                    "statuses": 1
                }
            },
            {
                "$addFields": {
                    "completed_tasks": {"$ifNull": ["$completed_tasks", 0]},
                    "completion_rate": {
                        "$cond": {
                            "if": {"$gt": ["$total_tasks", 0]},
                            "then": {"$multiply": [{"$divide": ["$completed_tasks", "$total_tasks"]}, 100]},
                            "else": 0
                        }
                    }
                }
            },
            {"$sort": {"project": 1}}
        ]
        
        results = list(collection.aggregate(pipeline))
        
        # Calcular métricas generales
        total_projects = len(results)
        total_tasks_all = sum(project.get("total_tasks", 0) for project in results)
        total_completed_all = sum(project.get("completed_tasks", 0) for project in results)
        
        summary = {
            "total_projects": total_projects,
            "total_tasks": total_tasks_all,
            "total_completed": total_completed_all,
            "overall_completion_rate": round((total_completed_all / total_tasks_all * 100) if total_tasks_all > 0 else 0, 1)
        }
        
        return {
            "projects": parse_json(results),
            "summary": summary
        }
        
    except Exception as e:
        logger.error(f"Error en get_project_status: {e}")
        return {
            "projects": [],
            "summary": {
                "total_projects": 0,
                "total_tasks": 0,
                "total_completed": 0,
                "overall_completion_rate": 0
            }
        }

# =======================================================
# ENDPOINTS DE MÉTRICAS
# =======================================================
@app.get("/api/metrics")
async def get_metrics():
    """Obtiene métricas generales del dashboard."""
    if not is_db_available():
        # Modo demo
        return {
            "total_tasks": 25,
            "completed_tasks": 10,
            "completion_rate": 40.0,
            "avg_completion_time": 7.5,
            "overdue_tasks": 5,
            "active_tasks": 15,
            "demo_mode": True
        }
    
    try:
        collection = db["tasks"]
        
        # Conteos básicos
        total_tasks = collection.count_documents({})
        completed_tasks = collection.count_documents({"status": "COMPLETED"})
        
        # Tareas vencidas
        now = datetime.now()
        overdue_tasks = collection.count_documents({
            "end": {"$lt": now},
            "status": {"$ne": "COMPLETED"}
        })
        
        # Tareas activas (en progreso o pendientes)
        active_tasks = collection.count_documents({
            "status": {"$in": ["IN_PROGRESS", "TO_DO", "PENDING"]}
        })
        
        return {
            "total_tasks": total_tasks,
            "completed_tasks": completed_tasks,
            "completion_rate": round((completed_tasks / total_tasks * 100) if total_tasks > 0 else 0, 1),
            "avg_completion_time": 7.5,  # Valor fijo por ahora
            "overdue_tasks": overdue_tasks,
            "active_tasks": active_tasks,
            "demo_mode": False
        }
        
    except Exception as e:
        logger.error(f"Error en get_metrics: {e}")
        return {
            "total_tasks": 0,
            "completed_tasks": 0,
            "completion_rate": 0,
            "avg_completion_time": 0,
            "overdue_tasks": 0,
            "active_tasks": 0
        }

@app.get("/api/metrics/summary")
async def get_metrics_summary():
    """Alias para /api/metrics."""
    return await get_metrics()

# =======================================================
# ENDPOINT CARGA DE TRABAJO
# =======================================================
@app.get("/api/tasks/workload")
async def get_workload_data():
    """Obtiene datos para el gráfico de carga de trabajo."""
    if not is_db_available():
        # Modo demo
        return parse_json([
            {
                "raw_user_id": "USER_1",
                "display_user_id": "Usuario 1",
                "total_tasks": 8,
                "TO_DO": 3,
                "IN_PROGRESS": 4,
                "BLOCKED": 1,
                "completed_tasks": 5,
                "completion_rate": 62.5,
                "overdue_tasks": 2
            },
            {
                "raw_user_id": "USER_2",
                "display_user_id": "Usuario 2",
                "total_tasks": 6,
                "TO_DO": 2,
                "IN_PROGRESS": 3,
                "BLOCKED": 1,
                "completed_tasks": 3,
                "completion_rate": 50.0,
                "overdue_tasks": 1
            }
        ])
    
    try:
        # Agrupar por usuario y estado
        pipeline = [
            {
                "$match": {
                    "user": {"$exists": True, "$ne": None}
                }
            },
            {
                "$group": {
                    "_id": {
                        "user": "$user",
                        "status": "$status"
                    },
                    "count": {"$sum": 1}
                }
            },
            {
                "$group": {
                    "_id": "$_id.user",
                    "statuses": {
                        "$push": {
                            "status": "$_id.status",
                            "count": "$count"
                        }
                    },
                    "total_tasks": {"$sum": "$count"}
                }
            },
            {
                "$project": {
                    "user": "$_id",
                    "statuses": 1,
                    "total_tasks": 1
                }
            },
            {"$sort": {"total_tasks": -1}}
        ]
        
        results = list(db["tasks"].aggregate(pipeline))
        
        # Formatear resultados
        formatted_results = []
        now = datetime.now()
        
        for result in results:
            user = result["user"] or "N/A"
            user_display = "Sin Asignar" if user == "N/A" else f"Usuario {user}" if user.isdigit() else user
            
            # Extraer conteos por estado
            status_counts = {
                "TO_DO": 0,
                "IN_PROGRESS": 0,
                "BLOCKED": 0,
                "COMPLETED": 0
            }
            
            for status_info in result.get("statuses", []):
                status = status_info.get("status", "").upper()
                count = status_info.get("count", 0)
                if status in status_counts:
                    status_counts[status] = count
            
            # Calcular tareas vencidas
            overdue_query = {
                "user": user,
                "end": {"$lt": now},
                "status": {"$nin": ["COMPLETED", "CANCELLED"]}
            }
            overdue_tasks = db["tasks"].count_documents(overdue_query)
            
            # Calcular tasa de finalización
            total = result["total_tasks"]
            completed = status_counts["COMPLETED"]
            completion_rate = round((completed / total * 100) if total > 0 else 0, 1)
            
            formatted_results.append({
                "raw_user_id": user,
                "display_user_id": user_display,
                "total_tasks": total,
                "TO_DO": status_counts["TO_DO"],
                "IN_PROGRESS": status_counts["IN_PROGRESS"],
                "BLOCKED": status_counts["BLOCKED"],
                "completed_tasks": completed,
                "completion_rate": completion_rate,
                "overdue_tasks": overdue_tasks
            })
        
        return parse_json(formatted_results)
        
    except Exception as e:
        logger.error(f"Error en get_workload_data: {e}")
        return parse_json([])

# =======================================================
# ENDPOINT SCOREBOARD DE EFICIENCIA
# =======================================================
@app.get("/api/efficiency/scoreboard")
async def get_efficiency_scoreboard():
    """Obtiene el scoreboard de eficiencia por usuario."""
    try:
        # Usar datos de workload
        workload_data = await get_workload_data()
        
        if isinstance(workload_data, dict) and "detail" in workload_data:
            # Hubo un error, devolver vacío
            return parse_json([])
        
        # Ordenar por tasa de finalización descendente
        if isinstance(workload_data, list):
            workload_data.sort(key=lambda x: x.get("completion_rate", 0), reverse=True)
        
        return parse_json(workload_data)
        
    except Exception as e:
        logger.error(f"Error en get_efficiency_scoreboard: {e}")
        return parse_json([])

# =======================================================
# ENDPOINTS DE SALUD Y UTILIDAD
# =======================================================
@app.get("/health")
async def health_check():
    """Endpoint de salud del sistema."""
    try:
        db_status = "disconnected"
        task_count = 0
        
        if client is not None and db is not None:
            try:
                client.admin.command('ping')
                db_status = "connected"
                task_count = db["tasks"].count_documents({})
            except:
                db_status = "disconnected"
        
        return {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "database": db_status,
            "tasks_in_db": task_count,
            "service": "dashboard-api",
            "version": "2.0.0"
        }
    except Exception as e:
        return {
            "status": "unhealthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "error": str(e),
            "service": "dashboard-api"
        }

@app.get("/favicon.ico")
async def favicon():
    """Evita errores 404 para favicon."""
    raise HTTPException(status_code=404, detail="No favicon configured")

@app.get("/")
async def root():
    """Página de inicio de la API."""
    endpoints = {
        "tasks": [
            "/api/tasks/all - Todas las tareas",
            "/api/tasks/overdue - Tareas vencidas",
            "/api/tasks/upcoming - Tareas próximas",
            "/api/tasks/daily - Tareas de hoy",
            "/api/tasks/gantt - Datos para Gantt",
            "/api/tasks/workload - Carga de trabajo"
        ],
        "metrics": [
            "/api/metrics - Métricas generales",
            "/api/project/status - Estado por proyecto",
            "/api/efficiency/scoreboard - Scoreboard"
        ],
        "ingestion": [
            "/api/ingest-csv - Subir CSV",
            "/api/ingest/tasks - Subir tareas (alternativo)"
        ],
        "system": [
            "/health - Salud del sistema",
            "/docs - Documentación Swagger"
        ]
    }
    
    # CORREGIDO: Usar is_db_available() en lugar de evaluar directamente db
    db_connected = is_db_available()
    
    return {
        "message": "🚀 Dashboard Tesina API - Funcionando",
        "version": "2.0.0",
        "status": "operational",
        "database": "connected" if db_connected else "disconnected",
        "endpoints": endpoints,
        "documentation": "/docs",
        "health_check": "/health"
    }

@app.get("/api/test")
async def test_endpoint():
    """Endpoint de prueba."""
    db_connected = is_db_available()
    
    return {
        "message": "API funcionando correctamente",
        "timestamp": datetime.now().isoformat(),
        "mongodb_connected": db_connected,
        "database": DB_NAME if db_connected else "No conectado",
        "status": "OK"
    }
