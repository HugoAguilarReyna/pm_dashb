from fastapi import FastAPI, HTTPException, Query
from pymongo import MongoClient
from pydantic import BaseModel
from datetime import datetime, timedelta
from typing import List, Optional
import os
from bson.objectid import ObjectId

# --- 1. Configuración de la Conexión a MongoDB ---
MONGO_URI = os.getenv('MONGO_URI', 'mongodb://mongodb:27017/')
DB_NAME = 'project_dashboard'

# --- 2. Inicialización de FastAPI y Conexión a DB ---
app = FastAPI(title="Tesina Dashboard API", version="1.0.0")

def get_mongo_db():
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        client.admin.command('ping')
        return client[DB_NAME]
    except Exception as e:
        print(f"Error connecting to MongoDB: {e}")
        raise HTTPException(status_code=503, detail="Database connection failed")

# --- 💡 FUNCIÓN HELPER PARA CONSTRUIR LA QUERY DE MONGO ---
def build_task_query(user_id: Optional[str] = None, status: Optional[str] = None) -> dict:
    """Construye un diccionario de query de MongoDB basado en los parámetros de filtro."""
    query = {}
    if user_id:
        # Asume que assigned_user_id está almacenado como string en la DB (ej: '1', '2')
        query['assigned_user_id'] = user_id 
    if status:
        query['status'] = status
    return query

# --- 3. Rutas de API ACTUALIZADAS CON FILTRADO ---

@app.get("/api/status")
def get_api_status():
    """Verifica si la API está viva y si MongoDB es accesible."""
    try:
        db = get_mongo_db()
        tasks_count = db['tasks'].count_documents({})
        return {"status": "ok", "db_status": "connected", "tasks_found": tasks_count}
    except HTTPException as e:
        return {"status": "error", "db_status": "failed", "detail": e.detail}


@app.get("/api/metrics")
def get_metrics(user_id: Optional[str] = Query(None)): # 💡 Captura el filtro global
    """Calcula y devuelve las métricas clave (KPIs) con filtro de usuario."""
    db = get_mongo_db()
    tasks_col = db['tasks']
    
    # 💡 APLICAR FILTRO
    query = build_task_query(user_id=user_id)
    
    total_tasks = tasks_col.count_documents(query)
    
    # Filtrar solo si hay tareas que contar
    if total_tasks == 0:
        return {
            "total_tasks": 0, "completed_tasks": 0, "completion_rate": 0, "avg_completion_time": 0
        }

    # Recalcular métricas con la query de filtro
    
    # Tareas completadas
    completed_query = query.copy()
    completed_query['status'] = "COMPLETED"
    completed_tasks = tasks_col.count_documents(completed_query)
    
    completion_rate = completed_tasks / total_tasks if total_tasks > 0 else 0
    
    # Calcular tiempo promedio de cierre (requiere iterar, más complejo con filtro)
    completed_tasks_data = tasks_col.find(completed_query)
    total_time_diff = 0
    count_completed = 0
    
    for task in completed_tasks_data:
        start = task.get('start_date')
        completion = task.get('completion_date')
        
        if start and completion and isinstance(start, datetime) and isinstance(completion, datetime):
            if completion > start:
                time_diff = completion - start
                total_time_diff += time_diff.total_seconds()
                count_completed += 1
    
    avg_completion_time_days = (total_time_diff / (3600 * 24)) / count_completed if count_completed > 0 else 0

    return {
        "total_tasks": total_tasks,
        "tasks_done": completed_tasks, # Renombrado para coincidir con el JS
        "tasks_to_do": tasks_col.count_documents({**query, "status": "TO_DO"}), # Añadir ToDo
        "tasks_overdue": tasks_col.count_documents({
            **query, 
            "due_date": {"$lt": datetime.now()}, 
            "status": {"$nin": ["COMPLETED", "CANCELLED"]}
        }), # Añadir Vencidas
        "completion_rate": completion_rate,
        "avg_completion_time": avg_completion_time_days
    }


@app.get("/api/project/status")
def get_project_status(user_id: Optional[str] = Query(None)): # 💡 Captura el filtro global
    """Calcula el conteo de tareas por estado para el gráfico de dona, con filtro de usuario."""
    db = get_mongo_db()
    tasks_col = db['tasks']
    
    # 💡 APLICAR FILTRO EN LA ETAPA $match
    match_query = build_task_query(user_id=user_id)
    
    pipeline = [
        {"$match": match_query}, 
        {"$group": {"_id": "$status", "count": {"$sum": 1}}},
        {"$project": {"status": "$_id", "count": 1, "_id": 0}}
    ]
    
    status_data = list(tasks_col.aggregate(pipeline))
    
    # Rellenar con estados que falten
    known_statuses = ['TO_DO', 'IN_PROGRESS', 'BLOCKED', 'COMPLETED', 'CANCELLED']
    current_statuses = {item['status'] for item in status_data}
    
    for status in known_statuses:
        if status not in current_statuses:
            status_data.append({"status": status, "count": 0})
            
    return status_data


@app.get("/api/tasks/overdue")
def get_overdue_tasks(user_id: Optional[str] = Query(None)): # 💡 Captura el filtro global
    """Devuelve las tareas vencidas y no completadas, con filtro de usuario."""
    db = get_mongo_db()
    tasks_col = db['tasks']
    
    today = datetime.now()
    
    # 💡 CONSTRUIR QUERY con filtro de usuario y condiciones de vencidas
    query = build_task_query(user_id=user_id)
    query.update({
        "due_date": {"$lt": today},
        "status": {"$nin": ["COMPLETED", "CANCELLED"]}
    })
    
    # Para el gráfico de barras vencidas por usuario (Frontend lo espera agrupado)
    pipeline = [
        {"$match": query},
        {"$group": {"_id": "$assigned_user_id", "overdue_count": {"$sum": 1}}},
        {"$project": {"user_id": "$_id", "overdue_count": 1, "_id": 0}}
    ]

    overdue_data = list(tasks_col.aggregate(pipeline))
    return overdue_data 


@app.get("/api/tasks/upcoming")
def get_upcoming_tasks(user_id: Optional[str] = Query(None)): # 💡 Captura el filtro global
    """Devuelve las tareas próximas a vencer (por día), con filtro de usuario."""
    db = get_mongo_db()
    tasks_col = db['tasks']
    
    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
    # Rango de una semana para mostrar tendencia, no solo "hoy y mañana"
    seven_days_end = today + timedelta(days=7) 
    
    # 💡 CONSTRUIR QUERY con filtro de usuario y condiciones de próximas
    query = build_task_query(user_id=user_id)
    query.update({
        "due_date": {"$gte": today, "$lt": seven_days_end},
        "status": {"$nin": ["COMPLETED", "CANCELLED"]}
    })
    
    # Agrupar por fecha de vencimiento (due_date) para el gráfico de líneas del frontend
    pipeline = [
        {"$match": query},
        {"$group": {
            "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$due_date"}}, 
            "count": {"$sum": 1}
        }},
        {"$project": {"date": "$_id", "count": 1, "_id": 0}},
        {"$sort": {"date": 1}}
    ]

    upcoming_data = list(tasks_col.aggregate(pipeline))
    return upcoming_data


@app.get("/api/tasks/gantt") # 💡 RENOMBRADO de /api/tasks/all
def get_gantt_data(
    status: Optional[str] = Query(None, description="Filtrar por estado (local)"),
    user_id: Optional[str] = Query(None, description="Filtrar por ID de usuario (local/global)")
):
    """Devuelve las tareas para el diagrama de Gantt y el gráfico de Carga de Trabajo, con filtros."""
    db = get_mongo_db()
    tasks_col = db['tasks']
    
    # 💡 APLICAR AMBOS FILTROS
    query = build_task_query(user_id=user_id, status=status)
        
    tasks = list(tasks_col.find(query))
    
    # Convertir Fechas y IDs a String ISO para JSON serializable (CRÍTICO)
    def convert_task(task):
        task_out = {}
        # Iterar sobre la tarea y convertir valores
        for key, value in task.items():
            if key == '_id':
                task_out['_id'] = str(value) # Convertir ObjectId a string
            elif isinstance(value, datetime):
                # 💡 SOLUCIÓN CRÍTICA 1: Forzar formato YYYY-MM-DD
                task_out[key] = value.strftime("%Y-%m-%d")
            else:
                task_out[key] = value
        
        # 💡 SOLUCIÓN CRÍTICA 2: Garantizar que existen los campos clave para D3.js
        # Esto previene el error 'undefined' que viste si un documento no tiene start_date/due_date
        task_out['id'] = task_out.get('_id', None) 
        task_out['start_date'] = task_out.get('start_date', None)
        task_out['due_date'] = task_out.get('due_date', None)
        
        return task_out
    
    return [convert_task(task) for task in tasks]

# LISTO 7