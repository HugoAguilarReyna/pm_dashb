# =======================================================
# 📅 TAREAS VENCIDAS (OVERDUE)
# =======================================================
@app.get("/api/tasks/overdue")
async def get_overdue_tasks():
    if not db:
        raise HTTPException(status_code=503, detail="Servicio de base de datos no disponible.")

    try:
        now = datetime.now().replace(hour=23, minute=59, second=59, microsecond=999999)
        
        # Filtramos por tareas que ya deberían haberse completado (end < now)
        # y que no están completadas
        query = {
            "end": {"$lt": now},
            "status": {"$ne": to_upper("COMPLETED")}
        }
        
        tasks = list(db["tasks"].find(query).sort("end", 1))  # Ordenar por fecha de fin
        return parse_json(tasks)
        
    except Exception as e:
        print(f"Error en get_overdue_tasks: {e}")
        raise HTTPException(status_code=500, detail=f"Error al obtener tareas vencidas: {e}")

# =======================================================
# 📊 ESTADO DEL PROYECTO
# =======================================================
@app.get("/api/project/status")
async def get_project_status():
    if not db:
        raise HTTPException(status_code=503, detail="Servicio de base de datos no disponible.")

    try:
        collection = db["tasks"]
        
        # Agrupar por proyecto y status
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
                            "cond": {"$eq": ["$$status.status", to_upper("COMPLETED")]}
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
            }
        ]
        
        results = list(collection.aggregate(pipeline))
        
        # Si no hay resultados, devolver estructura vacía
        if not results:
            return parse_json({"projects": [], "summary": {"total_projects": 0, "avg_completion_rate": 0}})
        
        # Calcular promedio de completion rate
        total_rate = sum(project.get("completion_rate", 0) for project in results)
        avg_rate = total_rate / len(results) if results else 0
        
        return parse_json({
            "projects": results,
            "summary": {
                "total_projects": len(results),
                "avg_completion_rate": round(avg_rate, 2)
            }
        })
        
    except Exception as e:
        print(f"Error en get_project_status: {e}")
        raise HTTPException(status_code=500, detail=f"Error al obtener estado del proyecto: {e}")

# =======================================================
# 📈 DATOS PARA GANTT
# =======================================================
@app.get("/api/tasks/gantt")
async def get_gantt_data(
    project: Optional[str] = Query(None, description="Filtrar por proyecto"),
    user: Optional[str] = Query(None, description="Filtrar por usuario"),
    status: Optional[str] = Query(None, description="Filtrar por estado")
):
    if not db:
        raise HTTPException(status_code=503, detail="Servicio de base de datos no disponible.")

    try:
        # Construir query dinámica
        query = {}
        
        if project:
            query["project"] = {"$regex": f"^{project}$", "$options": "i"}  # Búsqueda case-insensitive
            
        if user:
            query["user"] = {"$regex": f"^{user}$", "$options": "i"}
            
        if status:
            query["status"] = to_upper(status)
        
        # Campos necesarios para el Gantt
        projection = {
            "_id": 0,
            "id": 1,
            "text": 1,  # Nombre de la tarea
            "start": 1,
            "end": 1,
            "duration": 1,
            "progress": 1,
            "user": 1,
            "project": 1,
            "status": 1,
            "parent": 1
        }
        
        tasks = list(db["tasks"].find(query, projection).sort("start", 1))
        
        # Transformar a formato que espera el frontend Gantt
        gantt_data = []
        for task in tasks:
            # Asegurar que start y end sean datetime
            start_date = task.get("start")
            end_date = task.get("end")
            
            if not isinstance(start_date, datetime) or not isinstance(end_date, datetime):
                continue  # Saltar tareas sin fechas válidas
            
            # Calcular duración en días si no existe
            duration = task.get("duration")
            if not duration:
                duration_days = (end_date - start_date).days
                duration = max(duration_days, 1)  # Mínimo 1 día
            
            gantt_data.append({
                "id": task.get("id", ""),
                "text": task.get("text", ""),
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "duration": duration,
                "progress": task.get("progress", 0),
                "user": task.get("user", ""),
                "project": task.get("project", ""),
                "status": task.get("status", ""),
                "parent": task.get("parent", 0)
            })
        
        # También obtener lista única de proyectos y usuarios para filtros
        projects = db["tasks"].distinct("project")
        users = db["tasks"].distinct("user")
        
        return parse_json({
            "data": gantt_data,
            "filters": {
                "projects": [p for p in projects if p],
                "users": [u for u in users if u]
            }
        })
        
    except Exception as e:
        print(f"Error en get_gantt_data: {e}")
        raise HTTPException(status_code=500, detail=f"Error al obtener datos Gantt: {e}")

# =======================================================
# 📤 ENDPOINT DE INGESTA (COMPATIBILIDAD)
# =======================================================
@app.post("/api/ingest/tasks")
async def ingest_tasks_compatibility(file: UploadFile = File(...)):
    """Endpoint alternativo para compatibilidad con frontend existente"""
    return await ingest_csv_data(file)

# =======================================================
# 📊 CARGA DE TRABAJO (WORKLOAD)
# =======================================================
@app.get("/api/tasks/workload")
async def get_workload_data():
    if not db:
        raise HTTPException(status_code=503, detail="Servicio de base de datos no disponible.")

    try:
        # Agrupar tareas por usuario
        pipeline = [
            {
                "$group": {
                    "_id": "$user",
                    "total_tasks": {"$sum": 1},
                    "completed_tasks": {
                        "$sum": {
                            "$cond": [
                                {"$eq": ["$status", to_upper("COMPLETED")]},
                                1,
                                0
                            ]
                        }
                    },
                    "active_tasks": {
                        "$sum": {
                            "$cond": [
                                {"$in": ["$status", [to_upper("IN_PROGRESS"), to_upper("PENDING")]]},
                                1,
                                0
                            ]
                        }
                    },
                    "overdue_tasks": {
                        "$sum": {
                            "$cond": [
                                {
                                    "$and": [
                                        {"$ne": ["$status", to_upper("COMPLETED")]},
                                        {"$lt": ["$end", datetime.now()]}
                                    ]
                                },
                                1,
                                0
                            ]
                        }
                    }
                }
            },
            {
                "$project": {
                    "user": "$_id",
                    "total_tasks": 1,
                    "completed_tasks": 1,
                    "active_tasks": 1,
                    "overdue_tasks": 1,
                    "completion_rate": {
                        "$cond": {
                            "if": {"$gt": ["$total_tasks", 0]},
                            "then": {"$multiply": [{"$divide": ["$completed_tasks", "$total_tasks"]}, 100]},
                            "else": 0
                        }
                    }
                }
            },
            {"$sort": {"total_tasks": -1}}  # Ordenar por carga descendente
        ]
        
        results = list(db["tasks"].aggregate(pipeline))
        return parse_json(results)
        
    except Exception as e:
        print(f"Error en get_workload_data: {e}")
        raise HTTPException(status_code=500, detail=f"Error al obtener carga de trabajo: {e}")

# =======================================================
# 🎯 SCOREBOARD DE EFICIENCIA
# =======================================================
@app.get("/api/efficiency/scoreboard")
async def get_efficiency_scoreboard():
    if not db:
        raise HTTPException(status_code=503, detail="Servicio de base de datos no disponible.")

    try:
        pipeline = [
            {
                "$group": {
                    "_id": "$user",
                    "total_tasks": {"$sum": 1},
                    "completed_on_time": {
                        "$sum": {
                            "$cond": [
                                {
                                    "$and": [
                                        {"$eq": ["$status", to_upper("COMPLETED")]},
                                        {"$lte": ["$end", "$completed_at"]}  # Asumiendo que tienes completed_at
                                    ]
                                },
                                1,
                                0
                            ]
                        }
                    },
                    "avg_completion_time": {
                        "$avg": {
                            "$cond": [
                                {"$eq": ["$status", to_upper("COMPLETED")]},
                                {"$subtract": ["$completed_at", "$start"]},  # Diferencia en milisegundos
                                None
                            ]
                        }
                    }
                }
            },
            {
                "$project": {
                    "user": "$_id",
                    "total_tasks": 1,
                    "completed_on_time": 1,
                    "on_time_rate": {
                        "$cond": {
                            "if": {"$gt": ["$total_tasks", 0]},
                            "then": {"$multiply": [{"$divide": ["$completed_on_time", "$total_tasks"]}, 100]},
                            "else": 0
                        }
                    },
                    "avg_completion_days": {
                        "$cond": {
                            "if": {"$ne": ["$avg_completion_time", None]},
                            "then": {"$divide": ["$avg_completion_time", 1000 * 60 * 60 * 24]},  # Convertir a días
                            "else": 0
                        }
                    }
                }
            },
            {"$sort": {"on_time_rate": -1}}  # Ordenar por eficiencia descendente
        ]
        
        results = list(db["tasks"].aggregate(pipeline))
        
        # Si no hay campo completed_at, devolver datos básicos
        if not results or all(r["avg_completion_days"] == 0 for r in results):
            # Fallback a carga de trabajo
            return await get_workload_data()
        
        return parse_json(results)
        
    except Exception as e:
        print(f"Error en get_efficiency_scoreboard: {e}")
        # Fallback a carga de trabajo
        return await get_workload_data()
