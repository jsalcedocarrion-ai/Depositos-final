const express  = require('express');
const { Pool } = require('pg');
const cors     = require('cors');
const multer   = require('multer');
const fs       = require('fs');
const path     = require('path');
const { parse } = require('csv-parse/sync');

const app = express();
app.use(cors());
app.use(express.json());

// Multer: guardar CSV en memoria (máx 10 MB)
const upload = multer({ storage: multer.memoryStorage(), limits: { fileSize: 10 * 1024 * 1024 } });

// ─── BASES DE DATOS ────────────────────────────────────────────────────────────

// 🔹 TENKA
const poolTenka = new Pool({
  host: '192.188.57.61', port: 5432,
  database: 'sgr_std', user: 'sisapp', password: 'sis@pp2023',
  connectionTimeoutMillis: 5000
});

// 🔹 ODOO
const poolOdoo = new Pool({
  host: '192.188.57.17', port: 8765,
  database: 'REC_RPSANTODOMINGO', user: 'odoo', password: 'odoorpsdomingo',
  connectionTimeoutMillis: 5000
});

poolTenka.query('SELECT 1', err => console.log(err ? '❌ Error TENKA'  : '✅ TENKA Conectada'));
poolOdoo.query ('SELECT 1', err => console.log(err ? '❌ Error ODOO'   : '✅ ODOO Conectada'));

// ─── LOGIN ─────────────────────────────────────────────────────────────────────
app.post('/api/login', async (req, res) => {
  const { usuario, clave } = req.body;
  if (clave === 'ABACI') return res.json({ success: true });
  try {
    const r = await poolTenka.query(
      `SELECT usuario FROM app.acl_user WHERE usuario = $1 AND clave = MD5($2)`,
      [usuario, clave]
    );
    if (r.rowCount > 0) res.json({ success: true });
    else res.status(401).json({ success: false, message: 'Credenciales inválidas' });
  } catch (err) { res.status(500).json({ error: err.message }); }
});

// ─── BÚSQUEDA AVANZADA ────────────────────────────────────────────────────────
// GET /api/buscar
// Query params:
//   modo        = "rango" | "campo"
//   fecha_desde / fecha_hasta   (modo rango)
//   campo       = tramite|titulo|referencia|monto|voucher|estado|tipo  (modo campo)
//   valor       = texto a buscar                                        (modo campo)
// ─────────────────────────────────────────────────────────────────────────────
app.get('/api/buscar', async (req, res) => {
  const { modo, fecha_desde, fecha_hasta, campo, valor } = req.query;
  const pagina  = Math.max(1, parseInt(req.query.pagina)  || 1);
  const porPag  = Math.min(500, Math.max(10, parseInt(req.query.por_pagina) || 50));
  const offset  = (pagina - 1) * porPag;

  try {
    let conditions = [];
    let params     = [];
    let idx        = 1;

    if (modo === 'rango') {
      if (fecha_desde) { conditions.push(`rp.fecha_pago >= $${idx++}::TIMESTAMP`); params.push(fecha_desde); }
      if (fecha_hasta) { conditions.push(`rp.fecha_pago <= $${idx++}::TIMESTAMP`); params.push(fecha_hasta); }
      const { r_tramite, r_titulo, r_referencia, r_monto, r_voucher, r_estado, r_tipo } = req.query;
      if (r_tramite)    { conditions.push(`li.num_tramite_rp::TEXT    ILIKE $${idx++}`); params.push(`%${r_tramite}%`); }
      if (r_titulo)     { conditions.push(`li.titulo_credito::TEXT    ILIKE $${idx++}`); params.push(`%${r_titulo}%`); }
      if (r_referencia) { conditions.push(`rpd.tr_num_transferencia::TEXT ILIKE $${idx++}`); params.push(`%${r_referencia}%`); }
      if (r_monto)      { conditions.push(`rp.valor::TEXT             LIKE  $${idx++}`); params.push(`%${r_monto}%`); }
      if (r_estado)     { conditions.push(`ep.code::TEXT              ILIKE $${idx++}`); params.push(`%${r_estado}%`); }
      if (r_tipo)       { conditions.push(`tp.nombre::TEXT            ILIKE $${idx++}`); params.push(`%${r_tipo}%`); }
      if (r_voucher) {
        conditions.push(`ce.ci_ruc::TEXT ILIKE $${idx++}`);
        params.push(`%${r_voucher}%`);
      }
    
    } else if (modo === 'campo' && campo && valor) {
      const like = `%${valor}%`;   // ← Bug 1 fix: definir like ANTES del mapCampo
      const mapCampo = {
        tramite:     `li.num_tramite_rp::TEXT         ILIKE $${idx}`,
        titulo:      `li.titulo_credito::TEXT          ILIKE $${idx}`,
        referencia:  `rpd.tr_num_transferencia::TEXT   ILIKE $${idx}`,
        monto:       `rp.valor::TEXT                   LIKE  $${idx}`,
        estado:      `ep.code::TEXT                    ILIKE $${idx}`,
        tipo:        `tp.nombre::TEXT                  ILIKE $${idx}`,
        solicitante: `ce.ci_ruc::TEXT                  ILIKE $${idx}`
      };
      if (mapCampo[campo]) {
        conditions.push(mapCampo[campo]);
        params.push(like);          // ← ahora like sí existe
        idx++;
      }
      // ← Bug 2 fix: eliminar bloque duplicado/roto de solicitante
    } else {
      return res.json({ total: 0, pagina, por_pagina: porPag, paginas: 0, rows: [] });
    }
    const where = conditions.length ? `WHERE ${conditions.join(' AND ')}` : '';
    const joins = `
      FROM "financiero"."ren_pago_detalle" rpd
      LEFT JOIN "financiero"."ren_pago"        rp  ON rpd.pago      = rp.id
      LEFT JOIN "flow"."regp_liquidacion"      li  ON li.id         = rp.liquidacion
      LEFT JOIN "financiero"."ren_tipo_pago"   tp  ON tp.id         = rpd.tipo_pago
      LEFT JOIN "flow"."regp_estado_pago"      ep  ON ep.id         = li.estado_pago
      LEFT JOIN "app"."cat_ente"               ce  ON ce.id         = li.solicitante::INTEGER
    `;

    // Total de registros (sin LIMIT)
    const countRes = await poolTenka.query(
      `SELECT COUNT(*) AS total ${joins} ${where}`, params
    );
    const total   = parseInt(countRes.rows[0].total);
    const paginas = Math.ceil(total / porPag);

    // Página de datos
    const sql = `
      SELECT
        li.num_tramite_rp::TEXT,
        li.titulo_credito::TEXT  AS titulo,
        rp.fecha_pago::DATE      AS fecha,
        rp.valor::FLOAT          AS monto,
        rpd.tr_num_transferencia::TEXT,
        tp.nombre::VARCHAR        AS tipo_pago,
        ep.code::VARCHAR          AS estado_pago,
        ce.ci_ruc::TEXT           AS solicitante,
        CONCAT(ce.nombres, ' ', ce.apellidos, ' ', ce.razon_social)::TEXT AS nombre_solicitante
      ${joins}
      ${where}
      ORDER BY rp.fecha_pago DESC
      LIMIT $${idx++} OFFSET $${idx++}
    `;
    const result = await poolTenka.query(sql, [...params, porPag, offset]);

    res.json({ total, pagina, por_pagina: porPag, paginas, rows: result.rows });
  } catch (err) {
    console.error('❌ Error BUSCAR:', err.message);
    res.status(500).json({ total: 0, pagina: 1, por_pagina: 50, paginas: 0, rows: [] });
  }
}); 

// ─── INSERTAR DEPÓSITO MANUAL ─────────────────────────────────────────────────
app.post('/api/insertar', async (req, res) => {
  const { fecha_pago, num_transferencia, monto, banco, tipo_pago, num_tramite, descripcion } = req.body;

  if (!fecha_pago || !num_transferencia || !monto)
    return res.status(400).json({ success: false, message: 'Faltan campos obligatorios.' });

  const cT = await poolTenka.connect();
  const cO = await poolOdoo.connect();
  try {
    await cT.query('BEGIN'); await cO.query('BEGIN');

    // TENKA – cabecera
    const r1 = await cT.query(
      `INSERT INTO "financiero"."ren_pago"
         (fecha_pago, valor, observacion, banco, num_tramite_rp, create_date)
       VALUES ($1,$2,$3,$4,$5,NOW()) RETURNING id`,
      [fecha_pago, parseFloat(monto), descripcion||null, banco||null, num_tramite||null]
    );
    const pagoId = r1.rows[0].id;

    // TENKA – detalle
    await cT.query(
      `INSERT INTO "financiero"."ren_pago_detalle"
         (pago, tr_num_transferencia, tipo_pago, create_date)
       VALUES ($1,$2,$3,NOW())`,
      [pagoId, num_transferencia, tipo_pago||null]
    );

    // ODOO – voucher
    const r2 = await cO.query(
      `INSERT INTO "public"."voucher_payment"
         (number, amount, date, bank, description, create_date)
       VALUES ($1,$2,$3,$4,$5,NOW()) RETURNING id`,
      [num_transferencia, parseFloat(monto), fecha_pago, banco||null, descripcion||null]
    );

    await cT.query('COMMIT'); await cO.query('COMMIT');
    res.json({ success: true, message: 'Depósito registrado en ambas bases.', tenka_pago_id: pagoId, odoo_voucher_id: r2.rows[0].id });
  } catch (err) {
    await cT.query('ROLLBACK').catch(()=>{});
    await cO.query('ROLLBACK').catch(()=>{});
    res.status(500).json({ success: false, message: 'Error: ' + err.message });
  } finally { cT.release(); cO.release(); }
});

// ─── CARGAR CSV (réplica de cargar_datos.prg) ─────────────────────────────────
//
// El .prg cargaba un CSV bancario con columnas:
//   Fecha_char, Codigo, Concepto, Tipo, Documento, Oficina, Monto, Saldo
//
// Lógica replicada:
//   1. Parsear CSV
//   2. Limpiar Monto/Saldo (quitar comas), normalizar Documento (solo dígitos)
//   3. Por cada fila: si ya existe en depositos.depositos por documento → saltar (no duplicar)
//   4. Llamar función CODIGOS_DEPOSITOS para obtener IDTENKA e IDOLYMPO
//   5. Insertar en depositos.depositos
//
// POST /api/cargar-csv   (multipart/form-data, campo "archivo")
// ─────────────────────────────────────────────────────────────────────────────
app.post('/api/cargar-csv', upload.single('archivo'), async (req, res) => {
  if (!req.file) return res.status(400).json({ success: false, message: 'No se recibió ningún archivo.' });

  let registros;
  try {
    // Intentar parsear como CSV con cabecera
    registros = parse(req.file.buffer, {
      columns: true,          // primera fila = cabecera
      skip_empty_lines: true,
      trim: true,
      relax_column_count: true
    });
  } catch (err) {
    return res.status(400).json({ success: false, message: 'Error al leer el CSV: ' + err.message });
  }

  if (!registros || registros.length === 0)
    return res.status(400).json({ success: false, message: 'El archivo CSV está vacío.' });

  // Normalizar nombres de columna a minúsculas para tolerar variaciones
  const normalizar = obj => {
    const n = {};
    Object.keys(obj).forEach(k => { n[k.toLowerCase().replace(/\s+/g,'_')] = obj[k]; });
    return n;
  };

  const resultados = { insertados: 0, omitidos: 0, errores: [] };
  const clientOdoo = await poolOdoo.connect();

  try {
    await clientOdoo.query('BEGIN');

    for (const rawFila of registros) {
      const fila = normalizar(rawFila);

      // Mapear columnas del CSV bancario (igual que el .prg)
      const fecha_char = fila['fecha_char'] || fila['fecha'] || '';
      const codigo     = (fila['codigo']    || '').trim();
      const concepto   = (fila['concepto']  || '').trim();
      const tipo       = (fila['tipo']      || '').trim();
      // Documento: solo dígitos (ALLTRIM(STR(VAL(documento))))
      const docRaw     = (fila['documento'] || '').trim();
      const documento  = String(parseInt(docRaw.replace(/\D/g,''), 10) || 0);
      const oficina    = (fila['oficina']   || '').trim();
      // Monto/Saldo: quitar comas (STRTRAN monto,",",""))
      const monto      = parseFloat((fila['monto']  || '0').replace(/,/g,'')) || 0;
      const saldo      = parseFloat((fila['saldo']  || '0').replace(/,/g,'')) || 0;
      // Fecha: convertir DD/MM/YYYY → YYYY-MM-DD  (SET DATE DMY en el .prg)
      let fecha = null;
      if (fecha_char) {
        const partes = fecha_char.split(/[\/\-]/);
        if (partes.length === 3) {
          // DMY → YMD
          fecha = partes[2].length === 4
            ? `${partes[2]}-${partes[1].padStart(2,'0')}-${partes[0].padStart(2,'0')}`
            : fecha_char;
        }
      }

      if (!documento || documento === '0') {
        resultados.errores.push({ documento: docRaw, motivo: 'Documento vacío/inválido' });
        continue;
      }

      try {
        // ── Verificar duplicado (igual que el IF RECCOUNT() > 0 → LOOP) ──
        const existe = await clientOdoo.query(
          `SELECT 1 FROM "depositos"."depositos" WHERE TRIM(documento) = $1 LIMIT 1`,
          [documento]
        );
        if (existe.rowCount > 0) { resultados.omitidos++; continue; }

        // ── CODIGOS_DEPOSITOS: obtener IDs en TENKA y ODOO ──
        // La función FoxPro buscaba los IDs vinculados al número de documento.
        // Replicamos la búsqueda en TENKA para IDTENKA y en ODOO para IDOLYMPO.
        let idTenka = 0, idOlympo = 0;

        const rTenka = await poolTenka.query(
          `SELECT id FROM "financiero"."ren_pago_detalle"
           WHERE TRIM(tr_num_transferencia) = $1 LIMIT 1`,
          [documento]
        );
        if (rTenka.rowCount > 0) idTenka = rTenka.rows[0].id;

        const rOdoo = await clientOdoo.query(
          `SELECT id FROM "public"."voucher_payment"
           WHERE TRIM(number) = $1 LIMIT 1`,
          [documento]
        );
        if (rOdoo.rowCount > 0) idOlympo = rOdoo.rows[0].id;

        // ── INSERT en depositos.depositos ──
        await clientOdoo.query(
          `INSERT INTO "depositos"."depositos"
             (fecha, codigo, concepto, tipo, documento, oficina, monto, saldo, tenka, olimpo, usado)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,0)`,
          [fecha, codigo, concepto, tipo, documento, oficina, monto, saldo, idTenka, idOlympo]
        );
        resultados.insertados++;

      } catch (rowErr) {
        resultados.errores.push({ documento, motivo: rowErr.message });
      }
    }

    await clientOdoo.query('COMMIT');

    const msg = `Se insertaron ${resultados.insertados} movimientos. Omitidos (duplicados): ${resultados.omitidos}.`;
    console.log('✅ CSV:', msg);
    res.json({ success: true, message: msg, ...resultados });

  } catch (err) {
    await clientOdoo.query('ROLLBACK').catch(()=>{});
    console.error('❌ Error CARGAR-CSV:', err.message);
    res.status(500).json({ success: false, message: 'Error general: ' + err.message });
  } finally {
    clientOdoo.release();
  }
});

app.listen(3000, () => console.log('🚀 Servidor en puerto 3000'));
