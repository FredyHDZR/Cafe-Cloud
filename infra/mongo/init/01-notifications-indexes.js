// =============================================================================
// Café Cloud — colección `notifications` e índices (TICKET-001, ADR-005)
// =============================================================================
//
// El entrypoint de `mongo:7` ejecuta este archivo con mongosh UNA SOLA VEZ,
// cuando /data/db está vacío, y con `db` ya apuntando a MONGO_INITDB_DATABASE
// (`cafecloud`). Si el volumen `mongodata` ya existe, no se vuelve a ejecutar:
// para reaplicarlo, `docker compose down -v && docker compose up -d`.
//
// Los índices se crean aquí, en la infraestructura, y no en el arranque de
// notifier-service, porque el índice único sobre `event_id` es una restricción
// de integridad de la que dependen dos procesos distintos -notifier-service,
// que inserta, y cleanup-job, que borra- y ninguno de los dos debe ser dueño
// del esquema del otro. notifier-service COMPRUEBA que existen al arrancar
// (TICKET-006); no los crea.
//
// El script es idempotente: `createIndex` no falla si el índice ya existe con
// la misma definición.
// =============================================================================

const COLLECTION = 'notifications';

print('Café Cloud: preparando ' + db.getName() + '.' + COLLECTION);

// createCollection explícito para que la colección exista aunque todavía no se
// haya insertado ni un documento (el `getIndexes()` de la verificación lo pide).
if (!db.getCollectionNames().includes(COLLECTION)) {
  db.createCollection(COLLECTION);
  print('  colección creada');
} else {
  print('  la colección ya existía');
}

const notifications = db.getCollection(COLLECTION);

// 1. Deduplicación del consumidor bajo entrega at-least-once (ADR-004/ADR-005):
//    el mismo `orders.completed` entregado dos veces produce un DuplicateKeyError
//    que notifier-service trata como éxito.
notifications.createIndex(
  { event_id: 1 },
  { unique: true, name: 'uq_notifications_event_id' }
);

// 2. Consulta GET /notifications/{customer_id}, en orden de recencia.
notifications.createIndex(
  { customer_id: 1, created_at: -1 },
  { name: 'ix_notifications_customer_id_created_at' }
);

// 3. Barrido del cleanup-job: borrado de lo que supere las 24 h.
//    A propósito NO es un índice TTL: el borrado lo hace el job, que es un
//    componente exigido por el enunciado (ADR-005).
notifications.createIndex(
  { created_at: 1 },
  { name: 'ix_notifications_created_at' }
);

print('Café Cloud: índices de ' + COLLECTION + ' -> ' +
      notifications.getIndexes().map(function (i) { return i.name; }).join(', '));
