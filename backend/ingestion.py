import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import os
import io

print('=' * 60)
print('📊 INGESTIÓN DE DATOS A MONGO ATLAS - VERSIÓN ROBUSTA')
print('=' * 60)

# =======================================================
# ⚙️ CONFIGURACIÓN Y CONEXIÓN MONGO ATLAS
# =======================================================

# Usar variables de entorno (preferible) o la URI directa de Atlas
MONGO_ATLAS_URI = os.getenv(
    "MONGO_ATLAS_URI", 
    "mongodb+srv://aguilarhugo55_db_user:c5mfG11QT68ib4my@clusteract1.kpdhd5e.mongodb.net/?appName=ClusterAct1"
)
DB_NAME = os.getenv("DB_NAME", "dash_pm") 
CONNECTION_TIMEOUT_MS = 5000

# --- Función Auxiliar para Parsear Fechas de Forma Segura (Robusta) ---
def safe_date_conversion(date_value):
    """
    Convierte un valor a datetime naive (sin tz) con múltiples formatos:
    - %d/%m/%Y
    - %d/%m/%Y %H:%M:%S
    - %Y-%m-%d
    - %Y-%m-%d %H:%M:%S
    Fallback: pandas.to_datetime(dayfirst=True)
    """
    if date_value is None or pd.isnull(date_value):
        return None
    s = str(date_value).strip()
    if s == "":
        return None

    # Intentar formatos comunes
    for fmt in ["%d/%m/%Y", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d", "%Y-%m-%d %H:%M:%S"]:
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue

    # Fallback usando pandas (más tolerante)
    try:
        dt = pd.to_datetime(s, errors="coerce", dayfirst=True)
        if pd.notnull(dt):
            # Asegura que sea un objeto datetime de Python sin timezone
            return dt.to_pydatetime().replace(tzinfo=None) 
    except Exception:
        pass

    return None
# ------------------------------------------------------------


# 1. CONECTAR A MONGODB ATLAS
print('\n🔗 Conectando a MongoDB Atlas...')
try:
    client = MongoClient(MONGO_ATLAS_URI, serverSelectionTimeoutMS=CONNECTION_TIMEOUT_MS)
    # Ping de prueba
    client.admin.command('ping')
    print('✅ Conexión a MongoDB Atlas exitosa.')
except Exception as e:
    print(f'❌ Error de conexión a MongoDB Atlas: {e}')
    print('💡 Verifica la MONGO_ATLAS_URI y la configuración de red de Atlas.')
    exit(1)

# 2. SELECCIONAR BASE DE DATOS
db = client[DB_NAME]
tasks_col = db['tasks']
resources_col = db['resources']

print(f'✅ Base de datos: {db.name}')
try:
    print(f'✅ Colecciones disponibles (muestra): {db.list_collection_names()[:5]}')
except Exception:
    print('⚠️ No se pudo listar colecciones, pero la conexión es válida.')

# 3. LEER EL ARCHIVO CSV
# (La lógica de búsqueda de archivos se mantiene)
print('\n📂 Buscando archivo CSV...')
csv_path = None 
possible_paths = [
    'datos/insumo_tareas.csv',
    '../datos/insumo_tareas.csv',
    '../../datos/insumo_tareas.csv',
]

for path in possible_paths:
    if os.path.exists(path):
        csv_path = path
        print(f'✅ Archivo encontrado en: {path}')
        break
else:
    print('❌ No se encontró el archivo CSV en ninguna ruta')
    print('💡 Asegúrate de que el archivo exista en la ruta correcta.')
    client.close()
    exit(1)

# 4. LEER Y MOSTRAR DATOS DEL CSV
print('\n📄 Leyendo CSV...')
try:
    # Fuerza todas las columnas como texto y evita NaN
    df = pd.read_csv(csv_path, encoding='utf-8', dtype=str, keep_default_na=False) 
    print(f'✅ CSV leído: {len(df)} filas, {len(df.columns)} columnas')
    
    # Normalizar encabezados (opcional, pero buena práctica si el CSV tiene espacios)
    df.columns = df.columns.str.lower().str.replace(' ', '_').str.strip()

    print(f'📋 Columnas (normalizadas): {", ".join(df.columns.tolist())}')
    print('\n📄 Muestra del CSV (primeras 2 filas):')
    # Manejar caso donde las columnas no existan en el CSV
    cols_to_show = [col for col in ['task_description', 'start_date', 'due_date'] if col in df.columns]
    if cols_to_show:
        print(df[cols_to_show].head(2).to_string())
except Exception as e:
    print(f'❌ Error al leer CSV: {e}')
    client.close()
    exit(1)

# 5. INSERTAR DATOS EN MONGODB ATLAS
print('\n🔄 Insertando datos en MongoDB Atlas...')

tasks_inserted = 0
resources_inserted = set()
now = datetime.now().replace(microsecond=0) # Usar hora actual sin microsegundos

# Limpiar colecciones antes de insertar para asegurar datos frescos
tasks_col.delete_many({})
resources_col.delete_many({})
print('🧹 Colecciones limpiadas.')

for index, row in df.iterrows():
    # --------------------------------------------------
    # ✅ APLICACIÓN DE LA FUNCIÓN DE PARSEO DE FECHAS ROBUSTA
    # --------------------------------------------------
    # Parseo tolerante de fechas
    sd = safe_date_conversion(row.get('start_date'))
    dd = safe_date_conversion(row.get('due_date'))
    acd = safe_date_conversion(row.get('actual_completion_date'))

    # Documento base de la tarea
    task_doc = {
        'csv_index': index,
        'task_id': str(row.get('task_id', f'task_{index}')).strip(),
        'project_id': str(row.get('project_id', 'N/A')).strip(),
        'title': str(row.get('task_description', f'Tarea {index}')).strip(),
        'description': str(row.get('task_description', '')).strip(),
        'status': str(row.get('status', 'PENDING')).upper().strip(), # Estandarizar
        'is_milestone': str(row.get('is_milestone', '0')).strip() == '1',
        'assigned_to': str(row.get('assigned_user_id', '')).strip(),
        'priority': str(row.get('priority', 'medium')).lower().strip(),
        'user_role': str(row.get('user_role', 'team_member')).strip(),
        'dependencies': [dep.strip() for dep in str(row.get('dependencies', '')).split(',') if dep.strip()],
        'tags': [tag.strip() for tag in str(row.get('tags', '')).split(',') if tag.strip()],
        'created_at': now,
        'start_date': sd or now, # Fallback a created_at si la fecha de inicio es inválida
    }
    
    if dd:
        task_doc['due_date'] = dd
    if acd:
        task_doc['actual_completion_date'] = acd

    # 4. Effort Points (Puntos de esfuerzo)
    ep_raw = row.get('effort_points')
    if ep_raw:
        try:
            # Reemplazar ',' por '.' para permitir float y luego convertir a int
            task_doc['effort_points'] = int(float(str(ep_raw).replace(",", ".").strip()))
        except:
            task_doc['effort_points'] = 0
            
    # Insertar/Actualizar tarea
    try:
        tasks_col.update_one(
            {'task_id': task_doc['task_id']},
            {'$set': task_doc},
            upsert=True
        )
        tasks_inserted += 1
    except Exception as e:
        print(f'    ⚠️ Error insertando/actualizando tarea {index}: {str(e)[:50]}...')
    
    # Insertar/Actualizar recurso (usuario)
    user_id = task_doc['assigned_to']
    if user_id:
        if user_id not in resources_inserted:
            resource_doc = {
                'user_id': user_id,
                'role': task_doc['user_role'], 
                'updated_at': now
            }
            try:
                resources_col.update_one(
                    {'user_id': user_id},
                    {'$set': resource_doc},
                    upsert=True
                )
                resources_inserted.add(user_id)
            except Exception as e:
                print(f'    ⚠️ Error insertando recurso {user_id}: {str(e)[:50]}...')

print(f'\n✅ Proceso completado:')
print(f'    Tareas insertadas/actualizadas: {tasks_inserted}')
print(f'    Recursos únicos: {len(resources_inserted)}')

print('\n📊 ESTADÍSTICAS FINALES:')
print(f'    Total tareas en MongoDB: {tasks_col.count_documents({})}')
print(f'    Total recursos en MongoDB: {resources_col.count_documents({})}')

client.close()
print('\n🔌 Conexión cerrada')
print('=' * 60)
print('🎉 INGESTIÓN COMPLETADA CON ÉXITO')
print('=' * 60)