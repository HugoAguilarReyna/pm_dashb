# main.py - BACKEND COMPLETO PARA DASHBOARD TESINA

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

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Dashboard Tesina API",
    description="API para el dashboard de gestión de proyectos",
    version="1.0.0"
)

# --- Configuración CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Permitir todos los orígenes (en producción restringir)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Conexión MongoDB ---
mongo_uri = os.getenv("MONGO_ATLAS_URI") or os.getenv("MONGO_URI")
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
    
    # Comprobación de conexión
    client.admin.command('ping')
    logger.info(f"Conexión exitosa a MongoDB. Base de datos: {db_name}")
    
except Exception as e:
    logger.error(f"Error de conexión a MongoDB: {e}")
    logger.error(f"URI usada: {mongo_uri}")
    logger.error(f"Base de datos: {db_name}")

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
    if not date_value or str(date_value).lower() in ['nan', 'nat', 'none']:
        return None
    
    date_str = str(date_value).split('.')[0]  # Remueve milisegundos
    
    formats = [
        '%d/%m/%Y', '%d/%m/%Y %H:%M:%S',
        '%Y-%m-%d', '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%d %H:%M:%S%z',
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

# =======================================================
# ENDPOINTS DE INGESTA
# =======================================================
@app.post("/api/ingest-csv")
async def ingest_csv_data(file: UploadFile = File(...)):
    """Endpoint original para ingestión de CSV."""
    if not db:
        raise HTTPException(status_code=503, detail="Servicio de base de datos no disponible.")
        
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Solo se permiten archivos CSV.")

    try:
        content = await file.read()
        csv_file = io.StringIO(content.decode('utf-8'))
        df = pd.read_csv(csv_file)
        
        # Limpieza y estandarización de encabezados
        df.columns = df.columns.str.strip().str.replace(' ', '_').str.lower()
        
        # Renombrar columnas clave
        column_mapping = {
            'task_id': 'id',
            'project_name': 'project',
            'status': 'status',
            'due_date': 'end',
            'start_date': 'start',
            'assigned_to': 'user',
            'assigned_user_id': 'user',
            'estimated_effort_hrs': 'effort_hrs',
            'description': 'text',
            'name': 'text',
            'title': 'text'
        }
        
        for old_col, new_col in column_mapping.items():
            if old_col in df.columns:
                df.rename(columns={old_col: new_col}, inplace=True)
        
        # Verificar columnas requeridas
        required_cols = ['id', 'project', 'text', 'status', 'start', 'end']
        missing_cols = [col for col in required_cols if col not in df.columns]
        if missing_cols:
            raise HTTPException(
                status_code=400,
                detail=f"Faltan columnas requeridas: {', '.join(missing_cols)}"
            )
        
        # Procesar fechas
        df['start'] = df['start'].apply(safe_date_parse)
        df['end'] = df['end'].apply(safe_date_parse)
        df.dropna(subset=['start', 'end'], inplace=True)
        
        # Estandarizar status
        df['status'] = df['status'].apply(to_upper)
        
        # Añadir campos adicionales si no existen
        if 'user' not in df.columns:
            df['user'] = 'N/A'
        
        # Preparar para MongoDB
        df = df.replace({np.nan: None})
        data_to_insert = df.to_dict('records')
        
        if not data_to_insert:
            raise HTTPException(status_code=400, detail="No hay datos válidos después de la limpieza.")
        
        # Insertar/actualizar en MongoDB
        collection = db["tasks"]
        updates = 0
        inserts = 0
        
        for record in data_to_insert:
            filter_query = {'id': record['id']}
            record.pop('_id', None)
            
            result = collection.replace_one(filter_query, record, upsert=True)
            
            if result.modified_count > 0:
                updates += 1
            elif result.upserted_id:
                inserts += 1
        
        # Registrar última actualización
        db["metadata"].replace_one(
            {"key": "last_update"},
            {"key": "last_update", "timestamp": datetime.now(timezone.utc)},
            upsert=True
        )
        
        return {
            "status": "success",
            "message": "Datos actualizados exitosamente",
            "total_records": len(data_to_insert),
            "inserted": inserts,
            "updated": updates
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
    if not db:
        raise HTTPException(status_code=503, detail="Servicio no disponible.")
    
    try:
        tasks = list(db["tasks"].find({}))
        formatted_tasks = [format_task_for_response(task) for task in tasks]
        return parse_json(formatted_tasks)
    except Exception as e:
        logger.error(f"Error en get_all_tasks: {e}")
        raise HTTPException(status_code=500, detail=f"Error al obtener tareas: {str(e)}")

@app.get("/api/tasks/overdue")
async def get_overdue_tasks():
    """Obtiene tareas vencidas no completadas."""
    if not db:
        raise HTTPException(status_code=503, detail="Servicio no disponible.")
    
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
                days_overdue = (now - task["end"]).days
                task["days_overdue"] = max(days_overdue, 1)
        
        formatted_tasks = [format_task_for_response(task) for task in tasks]
        return parse_json(formatted_tasks)
    except Exception as e:
        logger.error(f"Error en get_overdue_tasks: {e}")
        raise HTTPException(status_code=500, detail=f"Error al obtener tareas vencidas: {str(e)}")

@app.get("/api/tasks/upcoming")
async def get_upcoming_tasks(days: Optional[int] = Query(30, ge=1)):
    """Obtiene tareas próximas a vencer."""
    if not db:
        raise HTTPException(status_code=503, detail="Servicio no disponible.")
    
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
        raise HTTPException(status_code=500, detail=f"Error al obtener tareas próximas: {str(e)}")

@app.get("/api/tasks/daily")
async def get_daily_tasks():
    """Obtiene tareas para el día actual."""
    if not db:
        raise HTTPException(status_code=503, detail="Servicio no disponible.")
    
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
        raise HTTPException(status_code=500, detail=f"Error al obtener tareas diarias: {str(e)}")

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
    if not db:
        raise HTTPException(status_code=503, detail="Servicio no disponible.")
    
    try:
        # Construir query
        query = {}
        if status:
            query["status"] = to_upper(status)
        if user_id:
            query["user"] = user_id
        if project:
            query["project"] = {"$regex": f"^{project}$", "$options": "i"}
        
        tasks = list(db["tasks"].find(query))
        
        # Formatear para Gantt
        gantt_tasks = []
        for task in tasks:
            if not task.get("start") or not task.get("end"):
                continue
            
            # Calcular duración
            start_date = task["start"]
            end_date = task["end"]
            if isinstance(start_date, str):
                start_date = safe_date_parse(start_date)
            if isinstance(end_date, str):
                end_date = safe_date_parse(end_date)
            
            if not start_date or not end_date:
                continue
            
            duration_days = max((end_date - start_date).days, 1)
            
            gantt_task = {
                "_id": str(task.get("_id", "")),
                "id": task.get("id", ""),
                "name": task.get("text", task.get("name", "Tarea sin nombre")),
                "title": task.get("text", task.get("name", "Tarea sin nombre")),
                "start_date": start_date.isoformat() if isinstance(start_date, datetime) else start_date,
                "end_date": end_date.isoformat() if isinstance(end_date, datetime) else end_date,
                "due_date": end_date.isoformat() if isinstance(end_date, datetime) else end_date,
                "assigned_user_id": task.get("user", "N/A"),
                "assigned_to": task.get("user", "N/A"),
                "status": task.get("status", "TO_DO"),
                "user": task.get("user", "N/A"),
                "project": task.get("project", ""),
                "duration_days": duration_days,
                "progress": task.get("progress", 0),
                "text": task.get("text", task.get("name", "Tarea sin nombre"))
            }
            gantt_tasks.append(gantt_task)
        
        # Obtener filtros disponibles
        projects = db["tasks"].distinct("project")
        users = db["tasks"].distinct("user")
        statuses = db["tasks"].distinct("status")
        
        return {
            "data": gantt_tasks,
            "filters": {
                "projects": [p for p in projects if p],
                "users": [u for u in users if u],
                "statuses": [s for s in statuses if s]
            }
        }
        
    except Exception as e:
        logger.error(f"Error en get_gantt_data: {e}")
        raise HTTPException(status_code=500, detail=f"Error al obtener datos Gantt: {str(e)}")

# =======================================================
# ENDPOINT ESTADO DEL PROYECTO
# =======================================================
@app.get("/api/project/status")
async def get_project_status():
    """Obtiene el estado de todos los proyectos."""
    if not db:
        raise HTTPException(status_code=503, detail="Servicio no disponible.")
    
    try:
        collection = db["tasks"]
        
        # Pipeline para agrupar por proyecto y estado
        pipeline = [
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
        raise HTTPException(status_code=500, detail=f"Error al obtener estado del proyecto: {str(e)}")

# =======================================================
# ENDPOINTS DE MÉTRICAS
# =======================================================
@app.get("/api/metrics")
async def get_metrics():
    """Obtiene métricas generales del dashboard."""
    if not db:
        return {
            "total_tasks": 0,
            "completed_tasks": 0,
            "completion_rate": 0,
            "avg_completion_time": 0,
            "overdue_tasks": 0,
            "active_tasks": 0
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
        
        # Calcular tiempo promedio de finalización
        completed_tasks_list = list(collection.find(
            {"status": "COMPLETED", "start": {"$exists": True}, "end": {"$exists": True}}
        ))
        
        avg_completion_days = 0
        if completed_tasks_list:
            completion_times = []
            for task in completed_tasks_list:
                if task.get("start") and task.get("end"):
                    start = task["start"] if isinstance(task["start"], datetime) else safe_date_parse(task["start"])
                    end = task["end"] if isinstance(task["end"], datetime) else safe_date_parse(task["end"])
                    
                    if start and end:
                        days = (end - start).days
                        if days > 0:
                            completion_times.append(days)
            
            if completion_times:
                avg_completion_days = sum(completion_times) / len(completion_times)
        
        return {
            "total_tasks": total_tasks,
            "completed_tasks": completed_tasks,
            "completion_rate": round((completed_tasks / total_tasks * 100) if total_tasks > 0 else 0, 1),
            "avg_completion_time": round(avg_completion_days, 1),
            "overdue_tasks": overdue_tasks,
            "active_tasks": active_tasks
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
    if not db:
        raise HTTPException(status_code=503, detail="Servicio no disponible.")
    
    try:
        # Agrupar por usuario y estado
        pipeline = [
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
                    "total_tasks": 1,
                    "to_do": {
                        "$arrayElemAt": [
                            {
                                "$filter": {
                                    "input": "$statuses",
                                    "as": "s",
                                    "cond": {"$eq": ["$$s.status", "TO_DO"]}
                                }
                            },
                            0
                        ]
                    },
                    "in_progress": {
                        "$arrayElemAt": [
                            {
                                "$filter": {
                                    "input": "$statuses",
                                    "as": "s",
                                    "cond": {"$eq": ["$$s.status", "IN_PROGRESS"]}
                                }
                            },
                            0
                        ]
                    },
                    "blocked": {
                        "$arrayElemAt": [
                            {
                                "$filter": {
                                    "input": "$statuses",
                                    "as": "s",
                                    "cond": {"$eq": ["$$s.status", "BLOCKED"]}
                                }
                            },
                            0
                        ]
                    }
                }
            },
            {
                "$addFields": {
                    "TO_DO": {"$ifNull": ["$to_do.count", 0]},
                    "IN_PROGRESS": {"$ifNull": ["$in_progress.count", 0]},
                    "BLOCKED": {"$ifNull": ["$blocked.count", 0]}
                }
            },
            {
                "$project": {
                    "user": 1,
                    "total_tasks": 1,
                    "TO_DO": 1,
                    "IN_PROGRESS": 1,
                    "BLOCKED": 1
                }
            },
            {"$sort": {"total_tasks": -1}}
        ]
        
        results = list(db["tasks"].aggregate(pipeline))
        
        # Formatear resultados
        formatted_results = []
        for result in results:
            user_display = "Sin Asignar" if result["user"] == "N/A" or not result["user"] else result["user"]
            formatted_results.append({
                "raw_user_id": result["user"],
                "display_user_id": user_display,
                "total_tasks": result["total_tasks"],
                "TO_DO": result.get("TO_DO", 0),
                "IN_PROGRESS": result.get("IN_PROGRESS", 0),
                "BLOCKED": result.get("BLOCKED", 0)
            })
        
        return parse_json(formatted_results)
        
    except Exception as e:
        logger.error(f"Error en get_workload_data: {e}")
        raise HTTPException(status_code=500, detail=f"Error al obtener carga de trabajo: {str(e)}")

# =======================================================
# ENDPOINT SCOREBOARD DE EFICIENCIA
# =======================================================
@app.get("/api/efficiency/scoreboard")
async def get_efficiency_scoreboard():
    """Obtiene el scoreboard de eficiencia por usuario."""
    if not db:
        raise HTTPException(status_code=503, detail="Servicio no disponible.")
    
    try:
        # Usar el endpoint de workload como base
        workload_data = await get_workload_data()
        
        if isinstance(workload_data, dict) and "detail" in workload_data:
            # Hubo un error
            return []
        
        # Calcular métricas adicionales
        now = datetime.now()
        for user_data in workload_data:
            user_id = user_data["raw_user_id"]
            
            # Tareas vencidas para este usuario
            overdue_query = {
                "user": user_id,
                "end": {"$lt": now},
                "status": {"$nin": ["COMPLETED", "CANCELLED"]}
            }
            user_data["overdue_tasks"] = db["tasks"].count_documents(overdue_query)
            
            # Tareas completadas para este usuario
            completed_query = {
                "user": user_id,
                "status": "COMPLETED"
            }
            user_data["completed_tasks"] = db["tasks"].count_documents(completed_query)
            
            # Calcular tasa de finalización
            total = user_data["total_tasks"]
            completed = user_data["completed_tasks"]
            user_data["completion_rate"] = round((completed / total * 100) if total > 0 else 0, 1)
        
        # Ordenar por tasa de finalización descendente
        workload_data.sort(key=lambda x: x["completion_rate"], reverse=True)
        
        return parse_json(workload_data)
        
    except Exception as e:
        logger.error(f"Error en get_efficiency_scoreboard: {e}")
        return []

# =======================================================
# ENDPOINTS DE SALUD Y UTILIDAD
# =======================================================
@app.get("/health")
async def health_check():
    """Endpoint de salud del sistema."""
    try:
        if client and db:
            # Verificar conexión a MongoDB
            client.admin.command('ping')
            db_status = "connected"
        else:
            db_status = "disconnected"
        
        return {
            "status": "healthy",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "database": db_status,
            "service": "dashboard-api"
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
    return {
        "message": "Dashboard Tesina API",
        "version": "1.0.0",
        "endpoints": {
            "tasks": [
                "/api/tasks/all",
                "/api/tasks/overdue",
                "/api/tasks/upcoming",
                "/api/tasks/daily",
                "/api/tasks/gantt",
                "/api/tasks/workload"
            ],
            "metrics": [
                "/api/metrics",
                "/api/project/status",
                "/api/efficiency/scoreboard"
            ],
            "ingestion": [
                "/api/ingest-csv",
                "/api/ingest/tasks"
            ],
            "health": "/health"
        },
        "documentation": "/docs"
    }

# =======================================================
# EJECUCIÓN PRINCIPAL
# =======================================================
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
