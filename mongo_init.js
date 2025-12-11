# Crea un archivo de inicialización para MongoDB
@"
db = db.getSiblingDB('admin');

// Verificar que el usuario admin existe
var adminUser = db.getUser('admin');
if (adminUser) {
  print('✅ Usuario admin existe');
} else {
  print('❌ Usuario admin NO existe');
}

// Crear la base de datos project_dashboard si no existe
use project_dashboard;
db.createCollection('tasks');
db.createCollection('resources');

// Crear un usuario específico para project_dashboard
db.createUser({
  user: 'project_user',
  pwd: 'tesina123',
  roles: [
    { role: 'readWrite', db: 'project_dashboard' },
    { role: 'dbAdmin', db: 'project_dashboard' }
  ]
});

print('✅ Base de datos project_dashboard inicializada');
print('✅ Usuario project_user creado');
"@ | Out-File -FilePath mongo_init.js -Encoding UTF8