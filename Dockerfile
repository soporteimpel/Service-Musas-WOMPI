# Usar imagen oficial de Node.js
FROM node:18-alpine

# Establecer directorio de trabajo
WORKDIR /app

# Copiar archivos de dependencias
COPY package*.json ./

# Instalar solo dependencias de producción
RUN npm ci --only=production && npm cache clean --force

# Copiar el resto de los archivos de la aplicación
COPY . .

# Exponer el puerto (Google Cloud Run usa PORT automáticamente)
EXPOSE 8080

# Usar variables de entorno para el puerto (Google Cloud Run requiere PORT)
ENV PORT=8080

# Comando para iniciar la aplicación
CMD ["node", "wompi-webhook.js"]

