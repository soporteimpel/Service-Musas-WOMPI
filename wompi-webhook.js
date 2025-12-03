const express = require('express');
const crypto = require('crypto');
const axios = require('axios');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 8080;

// Middleware para parsear JSON
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// Configuración de Rollbase
const ROLLBASE_API_BASE_URL = 'https://www.impeltechnology.com/rest/api';
const ROLLBASE_LOGIN_NAME = 'musas.conexion';
const ROLLBASE_LOGIN_PASSWORD = 'Impel2025*.';

// Secreto de eventos de Wompi (desde .env o config)
const WOMPI_EVENTS_SECRET = process.env.WOMPI_EVENTS_SECRET || 'test_events_qPsSg89RQkyHsQftdJdDW5EJfdXXvB4Z';

// Variable para almacenar el token de Rollbase
let rollbaseToken = '';
let tokenExpiration = 0;

/**
 * Obtener token de Rollbase
 */
async function getRollbaseToken() {
  // Si el token es válido y no ha expirado, retornarlo
  if (rollbaseToken && Date.now() < tokenExpiration) {
    return rollbaseToken;
  }

  try {
    const loginUrl = `${ROLLBASE_API_BASE_URL}/login`;
    const params = new URLSearchParams({
      loginName: ROLLBASE_LOGIN_NAME,
      password: ROLLBASE_LOGIN_PASSWORD,
      output: 'json'
    });

    const response = await axios.post(loginUrl, params.toString(), {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded'
      }
    });

    if (response.data && response.data.sessionId) {
      rollbaseToken = response.data.sessionId;
      // Token válido por 30 minutos
      tokenExpiration = Date.now() + (30 * 60 * 1000);
      return rollbaseToken;
    }

    throw new Error('No se pudo obtener el token de Rollbase');
  } catch (error) {
    throw error;
  }
}

/**
 * Ejecutar query en Rollbase
 */
async function executeRollbaseQuery(query, maxRows = 100) {
  const token = await getRollbaseToken();
  const queryUrl = `${ROLLBASE_API_BASE_URL}/selectQuery`;

  const params = new URLSearchParams({
    sessionId: token,
    query: query,
    maxRows: maxRows.toString(),
    output: 'json'
  });

  try {
    const response = await axios.post(queryUrl, params.toString(), {
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded'
      }
    });

    // Verificar si hay error en la respuesta
    if (response.data && response.data.status === 'fail') {
      const errorMsg = response.data.message || 'Error en SQL Query';
      throw new Error(`Rollbase SQL Error: ${errorMsg}`);
    }

    return response.data;
  } catch (error) {
    // Log detallado del error
    if (error.response) {
    } else {
    }
    throw error;
  }
}

/**
 * Crear registro en Rollbase usando create2
 */
async function createRollbaseRecord(objName, fields) {
  const token = await getRollbaseToken();
  const createUrl = `${ROLLBASE_API_BASE_URL}/create2`;

  const params = new URLSearchParams({
    objName: objName,
    sessionId: token,
    output: 'json'
  });

  // Agregar campos
  Object.entries(fields).forEach(([key, value]) => {
    if (value !== null && value !== undefined && value !== '') {
      params.append(key, String(value));
    }
  });

  try {
    const response = await axios.get(`${createUrl}?${params.toString()}`);
    return response.data;
  } catch (error) {
    throw error;
  }
}

/**
 * Crear registro en Rollbase usando create2
 */
async function createRollbaseRecord(objName, fields) {
  const token = await getRollbaseToken();
  const createUrl = `${ROLLBASE_API_BASE_URL}/create2`;

  const params = new URLSearchParams({
    objName: objName,
    sessionId: token,
    output: 'json'
  });

  // Agregar campos
  Object.entries(fields).forEach(([key, value]) => {
    if (value !== null && value !== undefined && value !== '') {
      params.append(key, String(value));
    }
  });

  try {
    const response = await axios.get(`${createUrl}?${params.toString()}`);
    return response.data;
  } catch (error) {
    throw error;
  }
}

/**
 * Actualizar registro en Rollbase usando update2
 */
async function updateRollbaseRecord(objName, recordId, fields) {
  const token = await getRollbaseToken();
  const updateUrl = `${ROLLBASE_API_BASE_URL}/update2`;

  // Construir parámetros base
  const baseParams = {
    objName: objName,
    sessionId: token,
    id: recordId,
    output: 'json'
  };

  // Agregar campos a actualizar (solo valores válidos)
  // Filtrar campos antes de agregarlos
  const validFields = {};
  Object.entries(fields).forEach(([key, value]) => {
    // Validar que el valor NO sea null, undefined, o string "undefined"/"null"
    const isNull = value === null;
    const isUndefined = value === undefined;
    
    // Verificar el tipo y valor antes de convertir a string
    let stringValue = null;
    if (!isNull && !isUndefined) {
      stringValue = String(value).trim();
      const isStringUndefined = stringValue === 'undefined' || stringValue === 'null' || 
                                stringValue.toLowerCase() === 'undefined' || 
                                stringValue.toLowerCase() === 'null';
      const isEmptyString = stringValue === '';
      
      if (!isStringUndefined && !isEmptyString) {
        validFields[key] = stringValue;
      }
    }
  });
  
  // Combinar todos los parámetros (base + campos)
  const allParams = { ...baseParams, ...validFields };
  
  // Construir URL usando URLSearchParams para codificación correcta
  const params = new URLSearchParams();
  Object.entries(allParams).forEach(([key, value]) => {
    // Asegurar que el valor sea string antes de agregar
    const stringValue = String(value);
    const isEmpty = !stringValue || stringValue.trim() === '' || stringValue === 'undefined' || stringValue === 'null';
    
    if (!isEmpty) {
      params.append(key, stringValue);
    }
  });

  const fullUrl = `${updateUrl}?${params.toString()}`;

  try {
    const response = await axios.get(fullUrl);
    return response.data;
  } catch (error) {
    if (error.response) {
      
      // Si hay un error específico sobre campos, mostrarlo
      if (error.response.data && error.response.data.message) {
      }
    }
    throw error;
  }
}

/**
 * Validar firma del webhook de Wompi
 * Según documentación: SHA256(event.id + event.created_at + events_secret)
 */
function validateWompiSignature(event, signature) {
  if (!WOMPI_EVENTS_SECRET || WOMPI_EVENTS_SECRET.trim() === '') {
    return true; // En desarrollo, permitir sin validación
  }

  try {
    const eventId = event.id || event.data?.id || '';
    const createdAt = event.created_at || event.data?.created_at || '';
    const dataToHash = `${eventId}${createdAt}${WOMPI_EVENTS_SECRET.trim()}`;

    const hash = crypto.createHash('sha256').update(dataToHash).digest('hex');
    const expectedSignature = hash;

    return signature === expectedSignature || signature === `sha256=${expectedSignature}`;
  } catch (error) {
    return false;
  }
}

/**
 * Buscar musa por ID (obtenido desde el email del cliente)
 * Estrategia: 
 * 1. Buscar la musa por email del cliente para obtener el musaId
 * 2. Buscar la musa directamente por ese musaId
 * 3. Verificar que Refencia_wompi coincida con la referencia del webhook
 * NOTA: La venta en Ventas se crea solo cuando el pago está aprobado, no antes
 */
async function findVentaByReference(reference, amountInCents, customerEmail) {
  
  try {
    // Escapar valores para evitar SQL injection
    const escapedEmail = customerEmail ? customerEmail.replace(/'/g, "''") : '';
    const escapedReference = reference.replace(/'/g, "''");
    
    let musaId = null;
    let planId = null;
    
    // PRIMERO: Buscar la musa por email del cliente para obtener el musaId
    if (escapedEmail) {
      try {
        // Buscar musa por email para obtener el ID
        // Buscar musa por email para obtener el ID (sin LIMIT)
        const musaQuery = `SELECT id FROM Musa WHERE Email = '${escapedEmail}'`;
        
        const result = await executeRollbaseQuery(musaQuery, 1);
        
        // Manejar diferentes formatos de respuesta
        let foundMusa = null;
        if (Array.isArray(result)) {
          foundMusa = result[0];
        } else if (result && typeof result === 'object') {
          foundMusa = result.records?.[0] || result.data?.[0] || result[0];
        }
        
        if (foundMusa) {
          musaId = Array.isArray(foundMusa) ? foundMusa[0] : (foundMusa.id || foundMusa.Id);
          
          if (musaId) {
            musaId = String(musaId);
          }
        } else {
        }
      } catch (error) {
      }
    }
    
    if (!musaId) {
      return null;
    }
    
    // SEGUNDO: Buscar la musa directamente por su ID y obtener Refencia_wompi para verificar
    try {
      const musaByIdQuery = `SELECT id, R74136898, Refencia_wompi FROM Musa WHERE id = '${musaId}' LIMIT 1`;
      
      const musaResult = await executeRollbaseQuery(musaByIdQuery);
      
      if (musaResult && Array.isArray(musaResult) && musaResult.length > 0) {
        const musa = Array.isArray(musaResult[0]) ? musaResult[0] : musaResult[0];
        
        // Extraer datos según el formato de respuesta
        const foundMusaId = Array.isArray(musa) ? musa[0] : musa.id;
        planId = Array.isArray(musa) ? musa[1] : (musa.R74136898 || musa['R74136898']);
        const musaReferencia = Array.isArray(musa) ? musa[2] : (musa.Refencia_wompi || musa['Refencia_wompi'] || musa.Referencia_wompi || musa['Referencia_wompi'] || musa.Reference_wompi || musa['Reference_wompi']);
        
        // TERCERO: Verificar que la referencia coincida
        if (foundMusaId === musaId) {
          if (musaReferencia && musaReferencia.trim() === reference.trim()) {
          } else {
            // Continuar aunque la referencia no coincida exactamente
          }
        }
      }
    } catch (error) {
    }
    
    if (!musaId) {
      return null;
    }
    
    // SEGUNDO: Buscar la venta asociada usando el musaId (campo R73564711)
    
    // Buscar la venta más reciente de esta musa
    // R73564711 = relación con tabla Musa (musaId)
    // R73887654 o planes = relación con tabla planes (planId)
    const ventaQueries = [
      { name: 'Ventas', query: `SELECT id, R73564711, R73887654, planes FROM Ventas WHERE R73564711 = '${musaId}' ORDER BY createdAt DESC LIMIT 1` },
      { name: 'Venta', query: `SELECT id, R73564711, R73887654, planes FROM Venta WHERE R73564711 = '${musaId}' ORDER BY createdAt DESC LIMIT 1` },
      { name: 'ventas', query: `SELECT id, R73564711, R73887654, planes FROM ventas WHERE R73564711 = '${musaId}' ORDER BY createdAt DESC LIMIT 1` }
    ];

    for (const { name: tableName, query } of ventaQueries) {
      try {
        const result = await executeRollbaseQuery(query);
        
        if (result && Array.isArray(result) && result.length > 0) {
          const venta = Array.isArray(result[0]) ? result[0] : result[0];
          
          if (venta) {
            const ventaId = Array.isArray(venta) ? venta[0] : venta.id;
            // El musaId ya lo tenemos, pero verificamos que coincida
            const ventaMusaId = Array.isArray(venta) ? venta[1] : (venta.R73564711 || venta['R73564711']);
            // Obtener planId desde R73887654 o planes (relación Ventas -> Planes)
            // Si no hay planId en la venta, usar el que obtuvimos de la musa
            const ventaPlanId = Array.isArray(venta) 
              ? (venta[2] || venta[3]) // Puede estar en posición 2 o 3 dependiendo del orden de campos
              : (venta.R73887654 || venta['R73887654'] || venta.planes || venta['planes']);
            
            // Usar el planId de la venta si está disponible, sino el de la musa
            const finalPlanId = ventaPlanId || planId;
            
            
            if (ventaId && musaId && ventaMusaId === musaId) {
              return { id: ventaId, musaId: musaId, planId: finalPlanId };
            } else {
            }
          }
        } else {
        }
      } catch (error) {
        continue;
      }
    }
    
    // Si encontramos la musa pero no la venta, aún podemos retornar el musaId y planId
    if (musaId) {
      return { id: null, musaId: musaId, planId: planId };
    }
    
    return null;
  } catch (error) {
    return null;
  }
}

/**
 * Endpoint principal del webhook de Wompi
 */
app.post('/webhook/wompi', async (req, res) => {
  try {
    const event = req.body;
    const signature = req.headers['x-signature'] || req.headers['signature'] || req.headers['X-Signature'] || '';

    // Validar firma del webhook
    const signatureValid = validateWompiSignature(event, signature);

    // Extraer datos del evento
    // Wompi puede enviar el evento en diferentes formatos:
    // 1. { event: "transaction.updated", data: { transaction: { id, reference, status, ... } } }
    // 2. { data: { id, reference, status, ... } }
    // 3. { id, reference, status, ... } directamente
    
    const eventData = event.data || event;
    const transaction = eventData.transaction || eventData;
    
    // Extraer y normalizar valores del webhook (asegurar que sean strings válidos, nunca undefined)
    const transactionId = (transaction.id || eventData.id || event.id || eventData.transaction?.id || '').toString().trim();
    const reference = (transaction.reference || eventData.reference || event.reference || eventData.transaction?.reference || '').toString().trim();
    const status = (transaction.status || eventData.status || event.status || eventData.transaction?.status || '').toString().trim();
    const amountInCents = transaction.amount_in_cents || eventData.amount_in_cents || event.amount_in_cents || eventData.transaction?.amount_in_cents || 0;
    const currency = (transaction.currency || eventData.currency || event.currency || eventData.transaction?.currency || 'COP').toString().trim();
    const customerEmail = (transaction.customer_email || eventData.customer_email || event.customer_email || eventData.transaction?.customer_email || '').toString().trim();
    const customerName = (transaction.customer_name || eventData.customer_name || event.customer_name || eventData.transaction?.customer_name || '').toString().trim();
    
    // Validar que los valores críticos no estén vacíos después de normalizar
    if (!reference || reference === 'undefined' || reference === 'null') {
      return res.status(400).json({ error: 'Referencia de transacción no válida' });
    }
    
    if (!status || status === 'undefined' || status === 'null') {
      return res.status(400).json({ error: 'Estado de transacción no válido' });
    }
    
    // Extraer musaId, planId, código de descuento y valor total desde customer-data si está disponible
    let musaIdFromWebhook = null;
    let planIdFromWebhook = null;
    let codigoDescuentoIdFromWebhook = null;
    let codigoDescuentoNombreFromWebhook = null;
    let valorTotalFromWebhook = null;
    try {
      // Intentar múltiples ubicaciones donde Wompi podría enviar customer-data
      const customerData = transaction.customer_data || 
                          transaction.customerData ||
                          eventData.customer_data || 
                          eventData.customerData ||
                          event.customer_data ||
                          event.customerData ||
                          eventData.transaction?.customer_data ||
                          eventData.transaction?.customerData ||
                          transaction.metadata?.customer_data ||
                          transaction.metadata?.customerData;
      
      if (customerData) {
        const customerDataObj = typeof customerData === 'string' ? JSON.parse(customerData) : customerData;
        musaIdFromWebhook = customerDataObj?.musaId || customerDataObj?.musa_id || null;
        planIdFromWebhook = customerDataObj?.planId || customerDataObj?.plan_id || null;
        codigoDescuentoIdFromWebhook = customerDataObj?.codigoDescuentoId || customerDataObj?.codigo_descuento_id || null;
        codigoDescuentoNombreFromWebhook = customerDataObj?.codigoDescuentoNombre || customerDataObj?.codigo_descuento_nombre || null;
        valorTotalFromWebhook = customerDataObj?.valorTotal || customerDataObj?.valor_total || null;
      }
    } catch (error) {
      // Continuar sin customer-data si hay error
    }
    
    // Intentar extraer musaId desde la referencia si está incluido
    // Formato esperado: MUSAS-*-*-*-MUSAID o MUSAS-timestamp-MUSAID
    if (!musaIdFromWebhook && reference) {
      try {
        // Buscar el patrón: último segmento después del último guion que sea numérico y tenga 6+ dígitos
        const referenceParts = reference.split('-');
        if (referenceParts.length > 1) {
          const lastPart = referenceParts[referenceParts.length - 1];
          // Si el último segmento es numérico y tiene más de 6 caracteres, podría ser el musaId
          if (/^\d{6,}$/.test(lastPart)) {
            // No lo usamos directamente, pero lo guardamos para referencia
          }
        }
      } catch (error) {
      }
    }

    if (!reference) {
      // Retornar 200 para que Wompi no reintente, pero loguear el error
      return res.status(200).json({ 
        error: 'Referencia no encontrada',
        receivedData: event
      });
    }

    if (!status) {
    }


    // ====================================================================
    // FLUJO COMPLETO DE ASOCIACIÓN:
    // 1. Se crea el registro de Musa con todos los datos (nombre, email, etc.)
    //    Rollbase devuelve un 'id' que es el musaId
    // 2. Se guarda la referencia de Wompi en el campo Refencia_wompi de la tabla Musa
    // 3. Se incluye el musaId en customer-data del checkout de Wompi
    // 4. El webhook recibe el musaId desde customer-data
    // 5. Busca la musa directamente por ese musaId
    // 6. Verifica que Refencia_wompi coincida con la referencia del webhook
    // 7. Obtiene el planId desde R74136898 de la musa
    // 8. Actualiza la tabla Musa con los datos del webhook usando ese musaId
    // NOTA: La venta en Ventas se crea solo cuando el pago está aprobado
    // ====================================================================
    
    // Buscar la musa usando el musaId del webhook si está disponible
    // Normalizar musaId desde el inicio para evitar undefined
    let musaId = musaIdFromWebhook ? String(musaIdFromWebhook).trim() : null;
    let ventaId = null;
    // Usar planId del webhook si está disponible, sino se buscará desde la musa
    let planId = planIdFromWebhook ? String(planIdFromWebhook).trim() : null;
    let planName = null;

    // PRIMERO: Buscar por Refencia_wompi directamente (SIEMPRE primero)
    try {
      const escapedReference = reference.replace(/'/g, "''");
      // Intentar diferentes nombres de campo posibles (sin LIMIT, Rollbase puede no soportarlo)
      const musaQueries = [
        `SELECT id, R74136898, Codigo_Descuento_Id, codigo_descuento_id, Codigo_Descuento_Nombre, Nombre_Codigo_Descuento, Valor_Total_Venta, valor_total_venta FROM Musa WHERE Refencia_wompi = '${escapedReference}'`,
        `SELECT id, R74136898, Codigo_Descuento_Id, codigo_descuento_id, Codigo_Descuento_Nombre, Nombre_Codigo_Descuento, Valor_Total_Venta, valor_total_venta FROM Musa WHERE Referencia_wompi = '${escapedReference}'`, // Fallback por si está mal escrito
        `SELECT id, R74136898, Codigo_Descuento_Id, codigo_descuento_id, Codigo_Descuento_Nombre, Nombre_Codigo_Descuento, Valor_Total_Venta, valor_total_venta FROM Musa WHERE Reference_wompi = '${escapedReference}'`,
        `SELECT id, R74136898, Codigo_Descuento_Id, codigo_descuento_id, Codigo_Descuento_Nombre, Nombre_Codigo_Descuento, Valor_Total_Venta, valor_total_venta FROM Musa WHERE referencia_wompi = '${escapedReference}'`,
        `SELECT id, R74136898, Codigo_Descuento_Id, codigo_descuento_id, Codigo_Descuento_Nombre, Nombre_Codigo_Descuento, Valor_Total_Venta, valor_total_venta FROM Musa WHERE name = '${escapedReference}'` // Por si está en name
      ];
      
      for (const musaQuery of musaQueries) {
        try {
          const musaByRefResult = await executeRollbaseQuery(musaQuery, 1);
          
          // Manejar diferentes formatos de respuesta
          let foundMusa = null;
          if (Array.isArray(musaByRefResult)) {
            foundMusa = musaByRefResult[0];
          } else if (musaByRefResult && typeof musaByRefResult === 'object') {
            foundMusa = musaByRefResult.records?.[0] || musaByRefResult.data?.[0] || musaByRefResult[0];
          }
          
          if (foundMusa) {
            // Extraer datos usando nombres de campos (más confiable que índices)
            const foundId = foundMusa.id || foundMusa.Id || foundMusa['id'] || (Array.isArray(foundMusa) ? foundMusa[0] : null);
            const foundPlanId = foundMusa.R74136898 || foundMusa['R74136898'] || (Array.isArray(foundMusa) ? foundMusa[1] : null);
            
            // Extraer código de descuento y valor total desde la musa usando nombres de campos
            if (!codigoDescuentoIdFromWebhook) {
              codigoDescuentoIdFromWebhook = foundMusa.Codigo_Descuento_Id || foundMusa['Codigo_Descuento_Id'] || 
                                            foundMusa.codigo_descuento_id || foundMusa['codigo_descuento_id'] ||
                                            (Array.isArray(foundMusa) ? foundMusa[2] : null);
              // Normalizar si es string
              if (codigoDescuentoIdFromWebhook) {
                codigoDescuentoIdFromWebhook = String(codigoDescuentoIdFromWebhook).trim();
                if (codigoDescuentoIdFromWebhook === 'null' || codigoDescuentoIdFromWebhook === 'undefined' || codigoDescuentoIdFromWebhook === '') {
                  codigoDescuentoIdFromWebhook = null;
                }
              }
            }
            
            if (!codigoDescuentoNombreFromWebhook) {
              codigoDescuentoNombreFromWebhook = foundMusa.Codigo_Descuento_Nombre || foundMusa['Codigo_Descuento_Nombre'] || 
                                                 foundMusa.Nombre_Codigo_Descuento || foundMusa['Nombre_Codigo_Descuento'] ||
                                                 (Array.isArray(foundMusa) ? foundMusa[4] : null);
              // Normalizar si es string
              if (codigoDescuentoNombreFromWebhook) {
                codigoDescuentoNombreFromWebhook = String(codigoDescuentoNombreFromWebhook).trim();
                if (codigoDescuentoNombreFromWebhook === 'null' || codigoDescuentoNombreFromWebhook === 'undefined' || codigoDescuentoNombreFromWebhook === '') {
                  codigoDescuentoNombreFromWebhook = null;
                }
              }
            }
            
            if (!valorTotalFromWebhook) {
              valorTotalFromWebhook = foundMusa.Valor_Total_Venta || foundMusa['Valor_Total_Venta'] || 
                                      foundMusa.valor_total_venta || foundMusa['valor_total_venta'] ||
                                      (Array.isArray(foundMusa) ? foundMusa[6] : null);
              // Normalizar si es string
              if (valorTotalFromWebhook) {
                valorTotalFromWebhook = String(valorTotalFromWebhook).trim();
                if (valorTotalFromWebhook === 'null' || valorTotalFromWebhook === 'undefined' || valorTotalFromWebhook === '') {
                  valorTotalFromWebhook = null;
                }
              }
            }
            
            if (foundId) {
              musaId = String(foundId).trim();
              // Solo actualizar planId si no lo tenemos del webhook y si se encontró en la musa
              if (!planId && foundPlanId) {
                planId = String(foundPlanId).trim();
              }
              break; // Salir del loop si encontramos
            }
          }
        } catch (error) {
          if (error.response) {
          }
          continue; // Intentar siguiente query
        }
      }
      
      if (!musaId) {
      }
    } catch (error) {
    }

    // SEGUNDO: Si tenemos musaId del webhook, buscar directamente por ID
    if (!musaId && musaIdFromWebhook) {
      try {
        const musaQuery = `SELECT id, R74136898, Refencia_wompi, Codigo_Descuento_Id, codigo_descuento_id, Codigo_Descuento_Nombre, Nombre_Codigo_Descuento, Valor_Total_Venta, valor_total_venta FROM Musa WHERE id = '${musaIdFromWebhook}' LIMIT 1`;
        const musaResult = await executeRollbaseQuery(musaQuery);
        
        if (musaResult && Array.isArray(musaResult) && musaResult.length > 0) {
          const musa = Array.isArray(musaResult[0]) ? musaResult[0] : musaResult[0];
          
          // Extraer datos usando nombres de campos (más confiable que índices)
          musaId = musa.id || musa.Id || musa['id'] || (Array.isArray(musa) ? musa[0] : null);
          
          // Extraer código de descuento y valor total desde la musa usando nombres de campos
          if (!codigoDescuentoIdFromWebhook) {
            codigoDescuentoIdFromWebhook = musa.Codigo_Descuento_Id || musa['Codigo_Descuento_Id'] || 
                                          musa.codigo_descuento_id || musa['codigo_descuento_id'] ||
                                          (Array.isArray(musa) ? musa[3] : null);
            // Normalizar si es string
            if (codigoDescuentoIdFromWebhook) {
              codigoDescuentoIdFromWebhook = String(codigoDescuentoIdFromWebhook).trim();
              if (codigoDescuentoIdFromWebhook === 'null' || codigoDescuentoIdFromWebhook === 'undefined' || codigoDescuentoIdFromWebhook === '') {
                codigoDescuentoIdFromWebhook = null;
              }
            }
          }
          
          if (!codigoDescuentoNombreFromWebhook) {
            codigoDescuentoNombreFromWebhook = musa.Codigo_Descuento_Nombre || musa['Codigo_Descuento_Nombre'] || 
                                               musa.Nombre_Codigo_Descuento || musa['Nombre_Codigo_Descuento'] ||
                                               (Array.isArray(musa) ? musa[5] : null);
            // Normalizar si es string
            if (codigoDescuentoNombreFromWebhook) {
              codigoDescuentoNombreFromWebhook = String(codigoDescuentoNombreFromWebhook).trim();
              if (codigoDescuentoNombreFromWebhook === 'null' || codigoDescuentoNombreFromWebhook === 'undefined' || codigoDescuentoNombreFromWebhook === '') {
                codigoDescuentoNombreFromWebhook = null;
              }
            }
          }
          
          if (!valorTotalFromWebhook) {
            valorTotalFromWebhook = musa.Valor_Total_Venta || musa['Valor_Total_Venta'] || 
                                   musa.valor_total_venta || musa['valor_total_venta'] ||
                                   (Array.isArray(musa) ? musa[7] : null);
            // Normalizar si es string
            if (valorTotalFromWebhook) {
              valorTotalFromWebhook = String(valorTotalFromWebhook).trim();
              if (valorTotalFromWebhook === 'null' || valorTotalFromWebhook === 'undefined' || valorTotalFromWebhook === '') {
                valorTotalFromWebhook = null;
              }
            }
          }
          
          // Solo actualizar planId si no lo tenemos del webhook y si se encontró en la musa
          if (!planId) {
            const foundPlanId = musa.R74136898 || musa['R74136898'] || (Array.isArray(musa) ? musa[1] : null);
            if (foundPlanId) {
              planId = String(foundPlanId).trim();
            }
          }
          
          const musaReferencia = musa.Refencia_wompi || musa['Refencia_wompi'] || musa.Referencia_wompi || musa['Referencia_wompi'] || musa.Reference_wompi || musa['Reference_wompi'] || (Array.isArray(musa) ? musa[2] : null);
          
          if (musaId) {
            musaId = String(musaId).trim();
          }
          
          if (musaReferencia && musaReferencia.trim() === reference.trim()) {
          } else {
          }
        }
      } catch (error) {
      }
    }

    // TERCERO: Si aún no tenemos musaId, buscar por referencia o email (fallback)
    if (!musaId) {
      const venta = await findVentaByReference(reference, amountInCents, customerEmail);
      
      if (venta) {
        // Obtener musaId desde la venta encontrada
        ventaId = venta.id;
        musaId = venta.musaId ? String(venta.musaId).trim() : null;
        planId = venta.planId ? String(venta.planId).trim() : null;
        
      }
    }

    // Si tenemos musaId pero no planId, buscar el planId desde la venta creada ANTES de redirigir a Wompi
    // La venta se crea con la referencia de Wompi en el campo 'name' ANTES de redirigir
    // IMPORTANTE: El planId se selecciona en "mi-plan" ANTES del checkout, así que debe estar en la venta
    if (musaId && !planId && reference) {
      try {
        // PRIMERO: Buscar la venta por la referencia (que se guarda en 'name' cuando se crea antes de redirigir)
        const escapedReference = reference.replace(/'/g, "''");
        // Usar solo R73887654 porque 'planes' puede no existir en la tabla Ventas
        const ventaQueries = [
          `SELECT R73887654 FROM Ventas WHERE name = '${escapedReference}' AND R73564711 = '${musaId}'`,
          `SELECT R73887654 FROM Ventas WHERE R73564711 = '${musaId}' AND name = '${escapedReference}'`,
          `SELECT id, R73887654 FROM Ventas WHERE name = '${escapedReference}'`
        ];
        
        let foundPlanId = null;
        for (const ventaQuery of ventaQueries) {
          try {
            const ventaResult = await executeRollbaseQuery(ventaQuery, 1);
            
            if (ventaResult && Array.isArray(ventaResult) && ventaResult.length > 0) {
              const venta = Array.isArray(ventaResult[0]) ? ventaResult[0] : ventaResult[0];
              
              // Extraer planId según el formato de respuesta
              if (Array.isArray(venta)) {
                // Si es array, el planId puede estar en la última posición
                foundPlanId = venta[venta.length - 1] || venta[1] || venta[0];
              } else {
                // Si es objeto, buscar en R73887654
                foundPlanId = venta.R73887654 || venta['R73887654'] || venta.planes || venta['planes'];
              }
              
              if (foundPlanId && foundPlanId !== null && foundPlanId !== 'null' && foundPlanId !== 'undefined') {
                planId = String(foundPlanId).trim();
                break;
              } else {
              }
            }
          } catch (error) {
            if (error.response) {
            }
            continue;
          }
        }
        
        // SEGUNDO: Si no se encontró por referencia, buscar la venta más reciente de esta musa que tenga planId
        if (!planId) {
          try {
            // Buscar solo por musaId, sin filtro de planes (porque puede fallar si el campo no existe)
            const ventaQuery = `SELECT R73887654 FROM Ventas WHERE R73564711 = '${musaId}'`;
            const ventaResult = await executeRollbaseQuery(ventaQuery, 1);
            
            if (ventaResult && Array.isArray(ventaResult) && ventaResult.length > 0) {
              // Tomar la primera venta encontrada
              const venta = Array.isArray(ventaResult[0]) ? ventaResult[0] : ventaResult[0];
              
              if (Array.isArray(venta)) {
                foundPlanId = venta[0];
              } else {
                foundPlanId = venta.R73887654 || venta['R73887654'];
              }
              
              if (foundPlanId && foundPlanId !== null && foundPlanId !== 'null' && foundPlanId !== 'undefined') {
                planId = String(foundPlanId).trim();
              } else {
              }
            } else {
            }
          } catch (error) {
            if (error.response) {
            }
          }
        }
      } catch (error) {
      }
    }

    // Obtener el nombre del plan desde la tabla Plan2, campo name
    if (planId) {
      try {
        // Solo buscar en Plan2
        const planQuery = `SELECT name FROM Plan2 WHERE id = '${planId}'`;
        const planNameResult = await executeRollbaseQuery(planQuery, 1);
        
        // Manejar diferentes formatos de respuesta
        let foundPlan = null;
        if (Array.isArray(planNameResult)) {
          foundPlan = planNameResult[0];
        } else if (planNameResult && typeof planNameResult === 'object') {
          foundPlan = planNameResult.records?.[0] || planNameResult.data?.[0] || planNameResult[0];
        }
        
        if (foundPlan) {
          // Extraer el nombre del plan
          planName = Array.isArray(foundPlan) ? foundPlan[0] : (foundPlan.name || foundPlan['name'] || foundPlan.Name || foundPlan['Name']);
          
          if (planName) {
            planName = String(planName).trim();
          } else {
          }
        } else {
        }
      } catch (error) {
        if (error.response) {
        }
      }
    } else {
    }

    // Si aún no tenemos musaId, intentar buscar por email (último recurso)
    if (!musaId && customerEmail) {
      try {
        // Buscar musa por email
        const musaQuery = `SELECT id, R74136898 FROM Musa WHERE Email = '${customerEmail.replace(/'/g, "''")}' LIMIT 1`;
        const musaResult = await executeRollbaseQuery(musaQuery);
        
        if (musaResult && Array.isArray(musaResult) && musaResult.length > 0) {
          const musa = Array.isArray(musaResult[0]) ? musaResult[0] : musaResult[0];
          musaId = Array.isArray(musa) ? musa[0] : musa.id;
          planId = Array.isArray(musa) ? musa[1] : (musa.R74136898 || musa['R74136898']);
          
          if (musaId) {
          }
        }
      } catch (error) {
      }
    }
    
    if (!musaId) {
    }
    
    // Si no tenemos musaId, no podemos actualizar la tabla Musa
    if (!musaId) {
    }

    // Actualizar tabla Musa con los campos del webhook (solo si tenemos musaId)
    // NOTA: NO se actualiza la tabla Ventas, solo se actualiza la tabla Musa
    if (musaId) {
      
      try {
        const timestamp = new Date().toISOString();
        
        // Crear objeto parcial con solo los campos relevantes del webhook (no todo el JSON completo)
        const wompiResponsePartial = {
          id: transactionId || null,
          reference: reference || null,
          status: status || null,
          amount_in_cents: amountInCents || null,
          currency: currency || null,
          customer_email: customerEmail || null,
          customer_name: customerName || null,
          created_at: transaction.created_at || eventData.created_at || event.created_at || eventData.transaction?.created_at || null,
          payment_method_type: transaction.payment_method_type || eventData.payment_method_type || event.payment_method_type || eventData.transaction?.payment_method_type || null
        };
        
        // Eliminar campos null del objeto antes de convertirlo a JSON
        Object.keys(wompiResponsePartial).forEach(key => {
          if (wompiResponsePartial[key] === null || wompiResponsePartial[key] === undefined) {
            delete wompiResponsePartial[key];
          }
        });
        
        // Convertir a JSON válido con solo los campos relevantes
        const wompiResponseJson = JSON.stringify(wompiResponsePartial);
        
        
        // NORMALIZACIÓN FINAL: Asegurar que todos los valores sean strings válidos o null (nunca undefined)
        // Esto previene que se guarden valores "undefined" como string en Rollbase
        const normalizedReference = (reference && reference !== 'undefined' && reference !== 'null') ? String(reference).trim() : null;
        const normalizedMusaId = (musaId && musaId !== 'undefined' && musaId !== 'null') ? String(musaId).trim() : null;
        const normalizedPlanId = (planId && planId !== 'undefined' && planId !== 'null') ? String(planId).trim() : null;
        const normalizedPlanName = (planName && planName !== 'undefined' && planName !== 'null') ? String(planName).trim() : null;
        const normalizedTimestamp = timestamp ? String(timestamp).trim() : null;
        const normalizedStatus = (status && status !== 'undefined' && status !== 'null') ? String(status).trim() : null;
        const normalizedCustomerEmail = (customerEmail && customerEmail !== 'undefined' && customerEmail !== 'null') ? String(customerEmail).trim() : null;
        const normalizedAmountInCents = (amountInCents && amountInCents !== 'undefined' && amountInCents !== 'null') ? String(amountInCents).trim() : null;
        const normalizedWompiResponse = wompiResponseJson ? String(wompiResponseJson).trim() : null;
        
        // Verificar que los valores base estén definidos antes de construir el objeto
        
        // Construir datos para actualizar en tabla Musa con los nombres exactos de campos
        // IMPORTANTE: Solo incluir campos con valores válidos (no vacíos, no null, no undefined)
        const musaUpdateData = {};
        
        // Función auxiliar para validar y agregar campos (todos los campos son tipo texto)
        const addFieldIfValid = (fieldName, value, required = false) => {
          // PASO 1: Verificar que no sea null o undefined PRIMERO (antes de cualquier conversión)
          if (value === null || value === undefined) {
            if (required) {
              throw new Error(`${fieldName} es requerido pero está ${value === null ? 'null' : 'undefined'}`);
            } else {
            }
            return false;
          }
          
          // PASO 2: Convertir a string de forma segura
          let stringValue;
          try {
            // Si ya es string, solo hacer trim
            if (typeof value === 'string') {
              stringValue = value.trim();
            } else {
              // Convertir a string y hacer trim
              stringValue = String(value).trim();
            }
          } catch (error) {
            if (required) {
              throw new Error(`Error al convertir ${fieldName} a string: ${error.message}`);
            }
            return false;
          }
          
          // PASO 3: Verificar que no sea string "undefined", "null", o vacío
          if (stringValue === '' || 
              stringValue === 'undefined' || 
              stringValue === 'null' || 
              stringValue.toLowerCase() === 'undefined' || 
              stringValue.toLowerCase() === 'null') {
            if (required) {
              throw new Error(`${fieldName} es requerido pero tiene valor inválido: "${stringValue}"`);
            } else {
            }
            return false;
          }
          
          // PASO 4: Solo agregar si pasó todas las validaciones (garantizar que es un string válido)
          musaUpdateData[fieldName] = stringValue; // Ya es string válido, no undefined, no null, no vacío
          return true;
        };
        
        // Campos a actualizar según los nombres exactos de Rollbase:
        // Reference, Musa_Id, Plan_Id, Plan_Name, Timestamp, Estatus
        // Usar valores normalizados (nunca undefined, solo null o string válido)
        
        // Reference: siempre debe tener valor
        addFieldIfValid('Reference', normalizedReference, true);
        
        // Musa_Id: siempre debe tener valor (nombre exacto del campo en Rollbase)
        addFieldIfValid('Musa_Id', normalizedMusaId, true);
        
        // Plan_Id: solo si está disponible (nombre exacto del campo en Rollbase)
        addFieldIfValid('Plan_Id', normalizedPlanId, false);
        
        // Plan_Name: solo si tiene valor (obtenido de tabla Planes, campo name)
        addFieldIfValid('Plan_Name', normalizedPlanName, false);
        
        // Timestamp: siempre debe tener valor
        addFieldIfValid('Timestamp', normalizedTimestamp, true);
        
        // Estatus: siempre debe tener valor
        addFieldIfValid('Estatus', normalizedStatus, true);
        
        // Customer_Email: solo si tiene valor (obtenido del webhook)
        addFieldIfValid('Customer_Email', normalizedCustomerEmail, false);
        
        // AmountInCents: solo si tiene valor (obtenido del webhook)
        addFieldIfValid('AmountInCents', normalizedAmountInCents, false);
        
        // WS_Response: NO se envía (solicitado por el usuario para probar)
        // addFieldIfValid('WS_Response', normalizedWompiResponse, true);
        
        
        // VERIFICAR CADA CAMPO INDIVIDUALMENTE ANTES DE ENVIAR
        Object.entries(musaUpdateData).forEach(([key, value]) => {
          const valueType = typeof value;
          const valueLength = value ? String(value).length : 0;
          const valuePreview = value ? String(value).substring(0, 50) : 'null/undefined';
          const isEmpty = !value || String(value).trim() === '';
          if (isEmpty || value === 'undefined' || value === 'null') {
          }
        });
        
        
        // Verificar que tenemos al menos algunos campos
        if (Object.keys(musaUpdateData).length === 0) {
          throw new Error('No hay campos válidos para actualizar');
        }
        
        // Verificar que los campos requeridos tengan valores válidos (SIN WS_Response)
        const requiredFields = ['Reference', 'Musa_Id', 'Timestamp', 'Estatus'];
        const missingFields = requiredFields.filter(field => !musaUpdateData[field] || String(musaUpdateData[field]).trim() === '');
        if (missingFields.length > 0) {
          throw new Error(`Campos requeridos faltantes: ${missingFields.join(', ')}`);
        }
        
        
        // ENVIAR todos los campos SIN WS_Response
        
        const fieldsToSend = {
          'Reference': musaUpdateData['Reference'],
          'Musa_Id': musaUpdateData['Musa_Id']
        };
        
        // Solo agregar Plan_Id si está disponible
        if (musaUpdateData['Plan_Id']) {
          fieldsToSend['Plan_Id'] = musaUpdateData['Plan_Id'];
        }
        
        // IMPORTANTE: Si el estado es APPROVED y tenemos planId, actualizar R74136898 (relación con Plan2)
        // Esto asegura que el plan activo se actualice correctamente en la tabla Musa
        if (status && status.toUpperCase() === 'APPROVED' && normalizedPlanId) {
          fieldsToSend['R74136898'] = normalizedPlanId;
        }
        
        // Solo agregar Customer_Email si está disponible
        if (musaUpdateData['Customer_Email']) {
          fieldsToSend['Customer_Email'] = musaUpdateData['Customer_Email'];
        }
        
        // Solo agregar AmountInCents si está disponible
        if (musaUpdateData['AmountInCents']) {
          fieldsToSend['AmountInCents'] = musaUpdateData['AmountInCents'];
        }
        
        // Solo agregar Plan_Name si está disponible
        if (musaUpdateData['Plan_Name']) {
          fieldsToSend['Plan_Name'] = musaUpdateData['Plan_Name'];
        }
        
        // Timestamp: siempre debe tener valor (es requerido)
        if (musaUpdateData['Timestamp']) {
          fieldsToSend['Timestamp'] = musaUpdateData['Timestamp'];
        }
        
        // Estatus: siempre debe tener valor (es requerido)
        if (musaUpdateData['Estatus']) {
          fieldsToSend['Estatus'] = musaUpdateData['Estatus'];
        }
        
        // WS_Response: NO se envía (solicitado por el usuario para probar)
        // if (musaUpdateData['WS_Response']) {
        //   fieldsToSend['WS_Response'] = musaUpdateData['WS_Response'];
        // }
        
        
        // Verificar que tengamos al menos algunos campos
        if (Object.keys(fieldsToSend).length === 0) {
          throw new Error('No hay campos para enviar a Rollbase');
        }
        
        try {
          // Enviar todos los campos juntos (SIN WS_Response)
          const updateResult = await updateRollbaseRecord('Musa', musaId, fieldsToSend);
          if (updateResult && updateResult.Msg) {
          }
          
        } catch (error) {
          if (error.response) {
          }
          // NO hacer throw para que el webhook continúe
        }
      } catch (error) {
        // Continuar aunque falle
      }
    } else {
    }

    // ====================================================================
    // CREAR VENTA EN TABLA Ventas SI EL ESTADO ES APPROVED
    // La venta se crea SOLO cuando el pago es aprobado, con código de descuento y valor total
    // ====================================================================
    if (status && status.toUpperCase() === 'APPROVED' && musaId && reference) {
      
      try {
        const ventaFields = {
          name: reference, // Campo name: referencia de Wompi
          R73564711: musaId // Relación con la tabla Musa
        };
        
        // Si tenemos planId, agregar relación con Plan2
        if (planId) {
          ventaFields.planes = String(planId); // Puede no existir, pero lo intentamos
          ventaFields.R73887654 = String(planId); // Campo que sí existe
        }
        
        // Si tenemos planName, agregar el nombre del plan a la venta
        if (planName) {
          ventaFields.Plan_Name = String(planName);
          ventaFields.plan_name = String(planName);
          ventaFields.Nombre_Plan = String(planName);
        }
        
        // Agregar código de descuento si está disponible desde customer-data o desde la musa
        if (codigoDescuentoIdFromWebhook) {
          // R73885532 - Relación con tabla Codigo_Descuento (ID del código de descuento aplicado)
          ventaFields.R73885532 = String(codigoDescuentoIdFromWebhook).trim();
          // Intentar también otros posibles nombres de campo para la relación
          ventaFields.Codigo_Descuento = String(codigoDescuentoIdFromWebhook).trim();
          ventaFields.codigo_descuento = String(codigoDescuentoIdFromWebhook).trim();
        }
        
        // Agregar nombre del código de descuento en campos de texto (no de relación)
        if (codigoDescuentoNombreFromWebhook) {
          ventaFields.Codigo_Descuento_Nombre = String(codigoDescuentoNombreFromWebhook).trim();
          ventaFields.Nombre_Codigo_Descuento = String(codigoDescuentoNombreFromWebhook).trim();
          ventaFields.Codigo_Descuento_Name = String(codigoDescuentoNombreFromWebhook).trim();
        }
        
        // Agregar valor total (precio final con descuento si aplica)
        if (valorTotalFromWebhook) {
          const valorTotalNum = parseFloat(valorTotalFromWebhook);
          if (!isNaN(valorTotalNum) && valorTotalNum > 0) {
            // Intentar múltiples variaciones del nombre del campo
            ventaFields.Valor_total = String(valorTotalNum);
            ventaFields.valor_total = String(valorTotalNum);
            ventaFields.ValorTotal = String(valorTotalNum);
            ventaFields.Valor_Total = String(valorTotalNum);
            ventaFields.valorTotal = String(valorTotalNum);
          }
        }
        
        // Consumidas_hoy: Inicializar en 0 al crear la venta
        ventaFields.Consumidas_hoy = '0';
        ventaFields.consumidas_hoy = '0';
        ventaFields.ConsumidasHoy = '0';
        
        // Intentar crear con diferentes nombres de objeto
        const objNames = ['Ventas', 'Venta', 'ventas'];
        for (const objName of objNames) {
          try {
            const ventaResult = await createRollbaseRecord(objName, ventaFields);
            
            if (ventaResult && ventaResult.id) {
              ventaId = ventaResult.id;
              break;
            } else if (ventaResult && Array.isArray(ventaResult) && ventaResult.length > 0) {
              ventaId = Array.isArray(ventaResult[0]) ? ventaResult[0][0] : ventaResult[0].id;
              break;
            }
          } catch (error) {
            // Continuar con el siguiente nombre de objeto si este falla
            continue;
          }
        }
      } catch (error) {
        // Continuar aunque falle
      }
    } else {
      if (status && status.toUpperCase() !== 'APPROVED') {
      } else if (!musaId) {
      } else if (!reference) {
      }
    }

    // Respuesta exitosa
    
    res.status(200).json({
      success: true,
      message: 'Webhook procesado correctamente',
      transactionId: transactionId,
      reference: reference,
      status: status,
      ventaId: ventaId,
      musaId: musaId,
      musaUpdated: !!musaId,
      ventaCreated: !!ventaId
    });

  } catch (error) {
    
    // Retornar 200 para que Wompi no reintente infinitamente
    res.status(200).json({
      success: false,
      error: 'Error interno del servidor',
      message: error.message,
      stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
    });
  }
});

// Endpoint de salud
app.get('/health', (req, res) => {
  res.status(200).json({ status: 'ok', service: 'wompi-webhook' });
});

// Iniciar servidor
app.listen(PORT, () => {
});

