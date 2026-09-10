// Implementa ADR-005. El entrypoint de `mongo:7` solo ejecuta este archivo cuando /data/db esta
// vacio: para reaplicarlo hace falta `docker compose down -v`.
//
// Los indices los crea la infraestructura y no notifier-service: el unico sobre `event_id` es una
// restriccion de la que dependen dos procesos, y ninguno debe ser dueño del esquema del otro.

const COLLECTION = 'notifications';

print('Café Cloud: preparando ' + db.getName() + '.' + COLLECTION);

// La coleccion se crea explicitamente para que exista antes del primer documento: el healthcheck
// del Compose cuenta sus indices.
if (!db.getCollectionNames().includes(COLLECTION)) {
  db.createCollection(COLLECTION);
  print('  colección creada');
} else {
  print('  la colección ya existía');
}

const notifications = db.getCollection(COLLECTION);

notifications.createIndex(
  { event_id: 1 },
  { unique: true, name: 'uq_notifications_event_id' }
);

notifications.createIndex(
  { customer_id: 1, created_at: -1 },
  { name: 'ix_notifications_customer_id_created_at' }
);

// Sin TTL a proposito: el borrado por retencion lo hace cleanup-job (ADR-005).
notifications.createIndex(
  { created_at: 1 },
  { name: 'ix_notifications_created_at' }
);

print('Café Cloud: índices de ' + COLLECTION + ' -> ' +
      notifications.getIndexes().map(function (i) { return i.name; }).join(', '));
