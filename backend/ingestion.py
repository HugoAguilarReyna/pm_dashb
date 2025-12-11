import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import os

print('=' * 60)
print('📊 INGESTIÓN DE DATOS - VERSIÓN ROBUSTA (Fechas corregidas)')
print('=' * 60)

# --- Función Auxiliar para Parsear Fechas de Forma Segura ---
def safe_date_parse(date_value):
    """
    Intenta convertir un valor (string o NaN) a datetime con formato DD/MM/YYYY.
    Retorna None si falla.
    """
    if pd.isnull(date_value) or not str(date_value).strip():
        return None
    try:
        # 🚀 CORRECCIÓN CLAVE: Usar strptime explícito para formato DD/MM/YYYY
        return datetime.strptime(str(date_value).strip(), '%d/%m/%Y')
    except ValueError:
        # Esto atrapa errores de formato, como si la fecha es inválida
        return None
# ------------------------------------------------------------


# 1. CONECTAR A MONGODB (adaptar la URI si es necesario)
print('\n🔗 Conectando a MongoDB...')
try:
    # Intenta sin autenticación primero
    client = MongoClient('mongodb://localhost:27017/', serverSelectionTimeoutMS=3000)
    client.admin.command('ping')
    print('✅ Conexión exitosa (sin autenticación)')
except Exception as e:
    print(f'❌ Error sin autenticación: {e}')
    print('💡 Intentando con autenticación (usando credenciales de ejemplo)...')
    try:
        # Intenta con autenticación (ejemplo: admin:tesina123)
        client = MongoClient('mongodb://admin:tesina123@localhost:27017/', serverSelectionTimeoutMS=3000)
        client.admin.command('ping')
        print('✅ Conexión exitosa (con autenticación)')
    except Exception as e2:
        print(f'❌ Error con autenticación: {e2}')
        print('💡 Saliendo...')
        exit(1)

# 2. SELECCIONAR BASE DE DATOS
db = client['project_dashboard']
tasks_col = db['tasks']
resources_col = db['resources']

print(f'✅ Base de datos: {db.name}')
print(f'✅ Colecciones disponibles: {db.list_collection_names()}')

# 3. LEER EL ARCHIVO CSV
print('\n📂 Buscando archivo CSV...')
csv_path = 'datos/insumo_tareas.csv' # Ruta por defecto

# Rutas comunes para desarrollo (ajustar según tu proyecto)
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
    # 📝 IMPORTANTE: Leemos sin parsear fechas, lo haremos manualmente en el loop
    df = pd.read_csv(csv_path, encoding='utf-8') 
    print(f'✅ CSV leído: {len(df)} filas, {len(df.columns)} columnas')
    print(f'📋 Columnas: {", ".join(df.columns.tolist())}')
    
    print('\n📄 Muestra del CSV (primeras 2 filas):')
    print(df[['task_description', 'start_date', 'due_date']].head(2).to_string())
except Exception as e:
    print(f'❌ Error al leer CSV: {e}')
    client.close()
    exit(1)

# 5. INSERTAR DATOS EN MONGODB
print('\n🔄 Insertando datos en MongoDB...')

tasks_inserted = 0
resources_inserted = set()

# Limpiar colecciones antes de insertar para asegurar datos frescos
tasks_col.delete_many({})
resources_col.delete_many({})
print('🧹 Colecciones limpiadas.')

for index, row in df.iterrows():
    # Documento base de la tarea con todos los campos
    task_doc = {
        'csv_index': index,
        'task_id': str(row.get('task_id', f'task_{index}')),
        'project_id': str(row.get('project_id', 'N/A')),
        'title': str(row.get('task_description', f'Tarea {index}')),
        'description': str(row.get('task_description', '')),
        'status': str(row.get('status', 'PENDING')).upper(), # Estandarizar a MAYÚSCULAS
        'is_milestone': str(row.get('is_milestone', '0')) == '1', # Convertir '1' a True
        'assigned_to': str(row.get('assigned_user_id', '')),
        'priority': str(row.get('priority', 'medium')).lower(),
        'user_role': str(row.get('user_role', 'team_member')),
        'dependencies': [dep.strip() for dep in str(row.get('dependencies', '')).split(',') if dep.strip()],
        'tags': [tag.strip() for tag in str(row.get('tags', '')).split(',') if tag.strip()],
        'created_at': datetime.now() 
    }
    
    # --------------------------------------------------
    # ✅ APLICACIÓN DE LA FUNCIÓN DE PARSEO DE FECHAS
    # --------------------------------------------------
    
    # 1. Start Date
    start_date = safe_date_parse(row.get('start_date'))
    if start_date:
        task_doc['start_date'] = start_date
    else:
        # Fallback a created_at si la fecha de inicio es inválida
        task_doc['start_date'] = task_doc['created_at'] 

    # 2. Due Date
    due_date = safe_date_parse(row.get('due_date'))
    if due_date:
        task_doc['due_date'] = due_date
        
    # 3. Actual Completion Date
    actual_completion_date = safe_date_parse(row.get('actual_completion_date'))
    if actual_completion_date:
        task_doc['actual_completion_date'] = actual_completion_date

    # 4. Effort Points (Puntos de esfuerzo)
    if 'effort_points' in df.columns and pd.notnull(row.get('effort_points')):
        try:
            task_doc['effort_points'] = int(row['effort_points'])
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
        print(f'    ⚠️ Error insertando/actualizando tarea {index}: {str(e)[:50]}...')
    
    # Insertar/Actualizar recurso (usuario)
    if 'assigned_user_id' in df.columns and row.get('assigned_user_id'):
        user_id = str(row['assigned_user_id'])
        if user_id not in resources_inserted:
            resource_doc = {
                'user_id': user_id,
                'role': str(row.get('user_role', 'team_member')), 
                'updated_at': datetime.now()
            }
            try:
                resources_col.update_one(
                    {'user_id': user_id},
                    {'$set': resource_doc},
                    upsert=True
                )
                resources_inserted.add(user_id)
            except Exception as e:
                print(f'    ⚠️ Error insertando recurso {user_id}: {str(e)[:50]}...')

print(f'\n✅ Proceso completado:')
print(f'    Tareas insertadas/actualizadas: {tasks_inserted}')
print(f'    Recursos únicos: {len(resources_inserted)}')

print('\n📊 ESTADÍSTICAS FINALES:')
print(f'    Total tareas en MongoDB: {tasks_col.count_documents({})}')
print(f'    Total recursos en MongoDB: {resources_col.count_documents({})}')

client.close()
print('\n🔌 Conexión cerrada')
print('=' * 60)
print('🎉 INGESTIÓN COMPLETADA CON ÉXITO')
print('=' * 60)