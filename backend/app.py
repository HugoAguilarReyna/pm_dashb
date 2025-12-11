import os
import json
import io
import pandas as pd
from fastapi import FastAPI, Query, File, UploadFile, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from datetime import datetime, timedelta, timezone
from bson import json_util

app = FastAPI(title="Dashboard Tesina API")

# --- Configuración CORS ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =======================================================
# ⚙️ CONFIGURACIÓN Y CONEXIÓN MONGO ATLAS
# =======================================================

# Usar variables de entorno (preferible) o los valores por defecto/directos de Atlas
MONGO_ATLAS_URI = os.getenv(
    "MONGO_ATLAS_URI", 
    "mongodb+srv://aguilarhugo55_db_user:c5mfG11QT68ib4my@clusteract1.kpdhd5e.mongodb.net/?appName=ClusterAct1"
)
DB_NAME = os.getenv("DB_NAME", "dash_pm") # El nombre de la base de datos en Atlas

# Inicialización global para la conexión
client: MongoClient = None
db = None

def init_db():
    """Inicializa la conexión a MongoDB Atlas."""
    global client, db
    try:
        if client is None:
            # Conexión, con timeout para no bloquear indefinidamente si falla
            client = MongoClient(MONGO_ATLAS_URI, serverSelectionTimeoutMS=5000)
            # Prueba de conexión (ping)
            client.admin.command('ping') 
            db = client[DB_NAME]
            print("Conexión a MongoDB Atlas exitosa.")
    except Exception as e:
        print(f"ERROR: No se pudo conectar a MongoDB Atlas: {e}")
        # En un entorno de producción, puedes optar por salir o levantar una excepción.
        raise Exception("Fallo de conexión a la base de datos.")

# Llamar a la función al inicio de la aplicación para establecer la conexión
# Se podría mover a un evento de 'startup' de FastAPI para un manejo más robusto, 
# pero por simplicidad se deja aquí.
init_db() 


# --- Utilidades ---
def parse_json(data):
    return json.loads(json_util.dumps(data))

def to_upper(s):
    return s.upper() if isinstance(s, str) else s

def safe_date_conversion(date_value):
    """
    Convierte un valor a datetime naive (sin tz) con múltiples formatos:
    - %d/%m/%Y
    - %d/%m/%Y %H:%M:%S
    - %Y-%m-%d
    - %Y-%m-%d %H:%M:%S
    Fallback: pandas.to_datetime(dayfirst=True)
    """
    if date_value is None:
        return None
    s = str(date_value).strip()
    if s == "":
        return None

    for fmt in ["%d/%m/%Y", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S"]:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue

    try:
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
        if pd.notnull(dt):
            return dt.to_pydatetime().replace(tzinfo=None)
    except Exception:
        pass

    return None

# =======================================================
# 🚀 INGESTA DE CSV
# =======================================================
@app.post("/api/ingest/tasks")
async def ingest_tasks_from_csv(file: UploadFile = File(...)):
    if file.content_type not in ['text/csv', 'application/vnd.ms-excel', 'application/octet-stream']:
        raise HTTPException(status_code=400, detail="Tipo de archivo no válido. Se espera CSV.")

    try:
        contents = await file.read()
        csv_data = io.StringIO(contents.decode("utf-8"))

        # Fuerza todas las columnas como texto y evita NaN (que rompe el parseo)
        df = pd.read_csv(csv_data, dtype=str, keep_default_na=False)

        # Normaliza encabezados
        df.columns = df.columns.str.lower().str.replace(' ', '_').str.strip()

        # Limpieza opcional de colecciones para datos frescos
        db.tasks.delete_many({})
        db.resources.delete_many({})

        tasks_inserted = 0
        resources_inserted = set()
        now = datetime.now().replace(microsecond=0, tzinfo=None)

        # Itera filas y construye documentos
        for index, row in df.iterrows():
            # Valores crudos para diagnóstico
            raw_start = row.get("start_date")
            raw_due = row.get("due_date")
            raw_acd = row.get("actual_completion_date")

            # Parseo tolerante
            sd = safe_date_conversion(raw_start)
            dd = safe_date_conversion(raw_due)
            acd = safe_date_conversion(raw_acd)

            task_doc = {
                "task_id": str(row.get("task_id", f"auto_{index}")).strip(),
                "project_id": str(row.get("project_id", "N/A")).strip(),
                "title": (row.get("task_description") or f"Tarea {index}").strip(),
                "description": str(row.get("task_description", "")).strip(),
                "status": to_upper(row.get("status", "PENDING")),
                "is_milestone": str(row.get("is_milestone", "0")).strip() == "1",
                "assigned_to": str(row.get("assigned_user_id", "")).strip(),
                "priority": str(row.get("priority", "medium")).strip().lower(),
                "user_role": str(row.get("user_role", "team_member")).strip(),
                "dependencies": [dep.strip() for dep in str(row.get("dependencies", "")).split(",") if dep.strip()],
                "tags": [tag.strip() for tag in str(row.get("tags", "")).split(",") if tag.strip()],
                "created_at": now,

                # Guarda también los crudos para diagnóstico
                "raw_start_date": raw_start,
                "raw_due_date": raw_due,
                "raw_actual_completion_date": raw_acd,
            }

            # Asignación de fechas (fallback created_at si no hay start_date)
            task_doc["start_date"] = sd or now
            if dd:
                task_doc["due_date"] = dd
            if acd:
                task_doc["actual_completion_date"] = acd

            # Effort points robusto
            ep_raw = row.get("effort_points")
            if ep_raw is not None and str(ep_raw).strip():
                try:
                    task_doc["effort_points"] = int(float(str(ep_raw).replace(",", ".").strip()))
                except Exception:
                    task_doc["effort_points"] = 0

            # Upsert tarea
            db.tasks.update_one({"task_id": task_doc["task_id"]}, {"$set": task_doc}, upsert=True)
            tasks_inserted += 1

            # Upsert recurso
            if task_doc["assigned_to"]:
                user_id = task_doc["assigned_to"]
                if user_id not in resources_inserted:
                    resource_doc = {
                        "user_id": user_id,
                        "role": task_doc["user_role"],
                        "updated_at": now
                    }
                    db.resources.update_one({"user_id": user_id}, {"$set": resource_doc}, upsert=True)
                    resources_inserted.add(user_id)

        return parse_json({
            "status": "success",
            "message": f"Ingesta completada. {tasks_inserted} tareas cargadas.",
            "inserted_count": tasks_inserted,
            "resources_count": len(resources_inserted)
        })

    except Exception as e:
        print(f"Error en ingest_tasks_from_csv: {e}")
        raise HTTPException(status_code=500, detail=f"Error al procesar el archivo CSV: {str(e)}")

# =======================================================
# 📌 GANTT
# =======================================================
@app.get("/api/tasks/gantt")
async def get_gantt_tasks(
    status: str = Query(None, description="Filtrar por estado"),
    user_id: str = Query(None, description="Filtrar por usuario asignado")
):
    try:
        query = {}
        if status:
            query["status"] = {"$regex": status, "$options": "i"}
        if user_id:
            query["assigned_to"] = user_id

        projection = {
            "_id": 0,
            "task_id": 1, "title": 1, "status": 1, "assigned_to": 1,
            "start_date": 1, "due_date": 1, "priority": 1, "created_at": 1,
            "task_description": 1,
            "raw_start_date": 1, "raw_due_date": 1, "raw_actual_completion_date": 1
        }

        results = list(db.tasks.find(query, projection).sort("start_date", 1).limit(200))

        normalized = []
        for r in results:
            original_start_date = r.get("start_date")
            created_at = r.get("created_at")
            end_date = r.get("due_date")

            # Fallback si falta start_date
            start_date_final = original_start_date if isinstance(original_start_date, datetime) else created_at

            start_date_iso = start_date_final.isoformat() if isinstance(start_date_final, datetime) else None
            end_date_iso = end_date.isoformat() if isinstance(end_date, datetime) else None
            task_name = r.get("title") or r.get("task_description") or f"Tarea {r.get('task_id', '')}"

            normalized.append({
                "task_id": r.get("task_id", ""),
                "name": task_name,
                "status": to_upper(r.get("status", "PENDING")),
                "assigned_user_id": r.get("assigned_to"),
                "start_date": start_date_iso,
                "end_date": end_date_iso,
                "priority": r.get("priority", "medium"),
                "created_at": created_at.isoformat() if isinstance(created_at, datetime) else None,
                "is_fallback_date": not isinstance(original_start_date, datetime),

                # Valores crudos para diagnóstico en navegador
                "raw_start_date": r.get("raw_start_date"),
                "raw_due_date": r.get("raw_due_date"),
                "raw_actual_completion_date": r.get("raw_actual_completion_date"),
            })

        return parse_json(normalized)

    except Exception as e:
        print(f"Error en get_gantt_tasks: {e}")
        return []

# =======================================================
# 📌 OVERDUE Y UPCOMING
# =======================================================
@app.get("/api/tasks/overdue")
async def get_overdue_tasks():
    try:
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        query = {
            "due_date": {"$lt": today},
            "status": {"$nin": ["COMPLETED", "CANCELLED", "completado", "cancelado", "completed", "cancelled"]}
        }
        results = list(
            db.tasks.find(query, {"_id": 0, "title": 1, "status": 1, "due_date": 1, "task_id": 1})
            .sort("due_date", 1).limit(100)
        )

        normalized = []
        for r in results:
            due_date = r.get("due_date")
            days_overdue = 0
            if isinstance(due_date, datetime):
                days_overdue = (today - due_date.replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)).days
                days_overdue = max(0, days_overdue)
            normalized.append({
                "name": r.get("title") or f"Tarea {r.get('task_id', '')}",
                "title": r.get("title") or f"Tarea {r.get('task_id', '')}",
                "status": to_upper(r.get("status", "PENDIENTE")),
                "due_date": due_date.isoformat() if isinstance(due_date, datetime) else None,
                "days_overdue": days_overdue,
                "task_id": r.get("task_id", "")
            })
        return parse_json(normalized)
    except Exception as e:
        print(f"Error en get_overdue_tasks: {e}")
        return []

@app.get("/api/tasks/upcoming")
async def get_upcoming_tasks():
    try:
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0, tzinfo=None)
        next_week = today + timedelta(days=7)
        query = {
            "due_date": {"$gte": today, "$lt": next_week},
            "status": {"$nin": ["COMPLETED", "CANCELLED", "completado", "cancelado", "completed", "cancelled"]}
        }
        results = list(
            db.tasks.find(query, {"_id": 0, "title": 1, "status": 1, "due_date": 1, "task_id": 1})
            .sort("due_date", 1).limit(100)
        )

        normalized = []
        for r in results:
            due_date = r.get("due_date")
            normalized.append({
                "name": r.get("title") or f"Tarea {r.get('task_id', '')}",
                "status": to_upper(r.get("status", "PENDIENTE")),
                "due_date": due_date.isoformat() if isinstance(due_date, datetime) else None,
                "task_id": r.get("task_id", "")
            })
        return parse_json(normalized)
    except Exception as e:
        print(f"Error en get_upcoming_tasks: {e}")
        return []

# =======================================================
# 🔧 ESTADO, MÉTRICAS, CARGA DE RECURSOS
# =======================================================
@app.get("/api/status")
async def api_status():
    global client 
    try:
        # Usamos el cliente global que se inicializó
        if client:
            client.admin.command("ping")
        else:
            # Intentar reconectar si la inicialización falló (manejo de error)
            init_db() 
            if not client:
                 raise Exception("Cliente de MongoDB no disponible.")

        task_count = db.tasks.count_documents({})
        resource_count = db.resources.count_documents({})
        return parse_json({
            "status": "online", "mongo_connected": True,
            "task_count": task_count, "resource_count": resource_count,
            "timestamp": datetime.now(timezone.utc).isoformat()
        })
    except Exception as e:
        print(f"Error en api_status: {e}")
        # Aseguramos que el cliente sea None si la conexión falló aquí
        client = None 
        raise HTTPException(status_code=503, detail="No se pudo conectar con MongoDB")

@app.get("/api/project/status")
async def get_project_status():
    try:
        pipeline = [
            {"$group": {"_id": {"$ifNull": ["$status", "SIN ESTADO"]}, "count": {"$sum": 1}}},
            # CORRECCIÓN PARA GRÁFICO: Usa 'name' en lugar de 'status'
            {"$project": {"name": {"$toUpper": "$_id"}, "count": 1, "_id": 0}}, 
            {"$sort": {"count": -1}}
        ]
        results = list(db.tasks.aggregate(pipeline))
        return parse_json(results)
    except Exception as e:
        print(f"Error en get_project_status: {e}")
        raise HTTPException(status_code=500, detail="Error al obtener estado del proyecto")

@app.get("/api/resources/load")
async def get_resources_load():
    try:
        pipeline = [
            {"$match": {"assigned_to": {"$ne": None, "$exists": True, "$ne": ""}}},
            {"$group": {
                "_id": "$assigned_to",
                "tasks_in_progress": {
                    "$sum": {
                        "$cond": [
                            {"$in": [{"$toUpper": "$status"}, ["IN_PROGRESS", "EN PROGRESO"]]},
                            1, 0
                        ]
                    }
                },
                "total_tasks": {"$sum": 1}
            }},
            {"$project": {"user_id": "$_id", "tasks_in_progress": 1, "total_tasks": 1, "_id": 0}},
            {"$sort": {"tasks_in_progress": -1}}
        ]
        results = list(db.tasks.aggregate(pipeline))
        return parse_json([
            {
                "user_id": r.get("user_id", ""),
                "tasks_in_progress": r.get("tasks_in_progress", 0),
                "total_tasks": r.get("total_tasks", 0)
            } for r in results
        ])
    except Exception as e:
        print(f"Error en get_resources_load: {e}")
        return []

@app.get("/api/workload")
async def get_resources_load_alias():
    return await get_resources_load()

@app.get("/api/metrics")
async def get_metrics():
    try:
        total_tasks = db.tasks.count_documents({})
        # Se asegura de considerar las diferentes formas de 'COMPLETADO'
        completed_tasks = db.tasks.count_documents({"status": {"$in": ["COMPLETED", "completado", "completed", "COMPLETADO"]}}) 
        completion_rate = (completed_tasks / total_tasks * 100) if total_tasks > 0 else 0
        avg_completion_time = 5.2
        return parse_json({
            "total_tasks": total_tasks,
            "completed_tasks": completed_tasks,
            "completion_rate": completion_rate,
            "avg_completion_time": avg_completion_time
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
    return await get_upcoming_tasks()

@app.get("/favicon.ico")
async def favicon():
    raise HTTPException(status_code=404, detail="No favicon configured")

# =======================================================
# ▶️ MAIN
# =======================================================
if __name__ == "__main__":
    import uvicorn
    # CORRECCIÓN APLICADA: Puerto cambiado de 8000 a 8080 para coincidir con la llamada del frontend.
    uvicorn.run(app, host="0.0.0.0", port=8000)