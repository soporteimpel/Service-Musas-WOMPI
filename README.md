# Wompi Webhook Backend

Backend para recibir webhooks de Wompi y actualizar Rollbase automáticamente.

## 🚀 Despliegue en Google Cloud Run

### Prerrequisitos

1. Tener instalado `gcloud CLI`
2. Tener un proyecto de Google Cloud configurado
3. Habilitar Cloud Run API en tu proyecto

### Opción 1: Despliegue rápido con Cloud Build

```bash
# Configurar el proyecto de Google Cloud
gcloud config set project TU_PROJECT_ID

# Construir y desplegar en un solo comando
gcloud run deploy wompi-webhook-backend \
  --source . \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars "WOMPI_EVENTS_SECRET=tu_secreto_aqui"
```

### Opción 2: Despliegue con Dockerfile

```bash
# 1. Construir la imagen Docker
docker build -t gcr.io/TU_PROJECT_ID/wompi-webhook-backend .

# 2. Subir la imagen a Google Container Registry
docker push gcr.io/TU_PROJECT_ID/wompi-webhook-backend

# 3. Desplegar en Cloud Run
gcloud run deploy wompi-webhook-backend \
  --image gcr.io/TU_PROJECT_ID/wompi-webhook-backend \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars "WOMPI_EVENTS_SECRET=tu_secreto_aqui"
```

### Opción 3: Usando Cloud Build (recomendado para CI/CD)

```bash
# Configurar Cloud Build
gcloud builds submit --tag gcr.io/TU_PROJECT_ID/wompi-webhook-backend

# Desplegar
gcloud run deploy wompi-webhook-backend \
  --image gcr.io/TU_PROJECT_ID/wompi-webhook-backend \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --set-env-vars "WOMPI_EVENTS_SECRET=tu_secreto_aqui"
```

## 🔧 Variables de Entorno

Configura estas variables en Google Cloud Run:

- `PORT`: Puerto del servidor (Cloud Run lo configura automáticamente, por defecto 8080)
- `WOMPI_EVENTS_SECRET`: Secreto para validar webhooks de Wompi

### Configurar variables de entorno en Cloud Run

```bash
gcloud run services update wompi-webhook-backend \
  --update-env-vars "WOMPI_EVENTS_SECRET=tu_secreto_aqui"
```

O desde la consola de Google Cloud:
1. Ve a Cloud Run → Selecciona el servicio
2. Edita y despliega nueva revisión
3. Variables y secretos → Agregar variable

## 📡 Endpoints

Una vez desplegado, tendrás acceso a:

- **Webhook principal**: `POST https://TU_SERVICIO.run.app/webhook/wompi`
- **Health check**: `GET https://TU_SERVICIO.run.app/health`

## 🔗 Configurar Webhook en Wompi

1. Ve al panel de Wompi
2. Configura el webhook URL: `https://TU_SERVICIO.run.app/webhook/wompi`
3. Asegúrate de usar el mismo `WOMPI_EVENTS_SECRET` en ambas partes

## 🧪 Desarrollo Local

```bash
# Instalar dependencias
npm install

# Ejecutar en modo desarrollo
npm run dev

# Ejecutar en producción
npm start
```

El servidor estará disponible en `http://localhost:8080`

## 📝 Notas

- El servicio está configurado para escuchar en el puerto 8080 (requerido por Cloud Run)
- Las credenciales de Rollbase están hardcodeadas en el código (líneas 15-16)
- Para producción, considera mover las credenciales a variables de entorno o Secret Manager

## 🔒 Seguridad

Para producción, considera:
1. Usar Google Secret Manager para credenciales sensibles
2. Habilitar autenticación en Cloud Run si es necesario
3. Configurar CORS apropiadamente
4. Validar siempre las firmas de los webhooks

