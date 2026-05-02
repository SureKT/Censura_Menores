// Cámara en vivo: detección client-side con BlazeFace, tracking IoU,
// clasificación backend vía Kafka, pixelado de menores en tiempo real.

const API1 = 'http://localhost:8001';
const API2 = 'http://localhost:8002';

const IOU_MATCH_THRESHOLD  = 0.25;
const PIXEL_BLOCK          = 12;
const CROP_JPEG_QUALITY    = 0.82;
const TRACK_LOST_FRAMES    = 20;
const RESEND_TIMEOUT_MS    = 5000;
const MIN_FACE_PROBABILITY = 0.90;  // descartar detecciones con baja confianza
const VISIBLE_FRAMES       = 3;     // solo dibujar/pixelar si la cara se vio en los últimos N frames
const MIN_CONFIRM_FRAMES   = 6;     // frames consecutivos necesarios para considerar la cara real (anti-parpadeo)

const videoEl   = document.getElementById('rtVideo');
const canvasEl  = document.getElementById('rtCanvas');
const ctx       = canvasEl.getContext('2d');
const btnStart  = document.getElementById('btnStart');
const btnStop   = document.getElementById('btnStop');
const statusEl  = document.getElementById('rtStatus');
const faceListEl= document.getElementById('faceList');
const statsEl   = document.getElementById('rtStats');
const sessionEl = document.getElementById('sessionInfo');
const logEl     = document.getElementById('rtLog');

// Scratch canvas para recortes
const cropCanvas = document.createElement('canvas');
const cropCtx    = cropCanvas.getContext('2d');

let sessionId    = null;
let detector     = null;
let stream       = null;
let rafId        = null;
let sse          = null;
let running      = false;
let frameCount      = 0;
let lastFpsTime     = performance.now();
let fps             = 0;
let rawLogDone      = false;   // log de estructura raw solo una vez por sesión

// Stats para el log de 1 segundo
let logStats = {
  framesProcessed: 0,
  detectedSum: 0,
  newFacesSent: 0,
  apiOk: 0,
  apiErr: 0,
  sseReceived: 0,
  errors: [],
};
let lastLogTime = performance.now();

const tracked = new Map();
let nextFaceNum = 1;

// ── Logging ────────────────────────────────────────────────────────────────
function log(msg) {
  const ts = new Date().toLocaleTimeString('es-ES', { hour12: false });
  const line = `[${ts}] ${msg}`;
  console.log(line);
  if (logEl) {
    const lines = logEl.textContent.split('\n').slice(0, 80);
    logEl.textContent = line + '\n' + lines.join('\n');
  }
}

function printSecondLog() {
  lastLogTime = performance.now();
  const avgDet = logStats.framesProcessed > 0
    ? (logStats.detectedSum / logStats.framesProcessed).toFixed(2) : '0';

  const trackedArr = [...tracked.values()];
  const pendingFaces = trackedArr.filter(f => f.status === 'pending')
    .map(f => `${f.id}(${Math.round((performance.now() - f.inflightTs)/1000)}s)`).join(', ') || '—';
  const classifiedFaces = trackedArr.filter(f => f.status === 'classified')
    .map(f => `${f.id}=${f.isMinor ? 'MENOR' : 'adulto'}(${f.age}a,${Math.round((f.confidence||0)*100)}%)`).join(', ') || '—';

  log(
    `FPS=${fps.toFixed(1)} | frames=${logStats.framesProcessed} | caras/frame=${avgDet} | ` +
    `tracked=${tracked.size} | enviadas=${logStats.newFacesSent} | ` +
    `API ok=${logStats.apiOk} err=${logStats.apiErr} | SSE_msgs=${logStats.sseReceived}`
  );
  if (tracked.size > 0) {
    log(`  pendientes: [${pendingFaces}]  clasificadas: [${classifiedFaces}]`);
  }
  if (logStats.errors.length > 0) {
    log(`  ERRORES: ${logStats.errors.slice(-3).join('; ')}`);
  }

  logStats = { framesProcessed: 0, detectedSum: 0, newFacesSent: 0, apiOk: 0, apiErr: 0, sseReceived: 0, errors: [] };
}

// ── Helpers ─────────────────────────────────────────────────────────────────
function confLabel(conf) {
  if (conf == null || conf <= 0) return 'sin modelo';
  return `${Math.round(conf * 100)}% conf.`;
}

function uuid() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
    const r = Math.random() * 16 | 0;
    const v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

function iou(a, b) {
  const x1 = Math.max(a.x, b.x);
  const y1 = Math.max(a.y, b.y);
  const x2 = Math.min(a.x + a.w, b.x + b.w);
  const y2 = Math.min(a.y + a.h, b.y + b.h);
  const inter = Math.max(0, x2 - x1) * Math.max(0, y2 - y1);
  const union = a.w * a.h + b.w * b.h - inter;
  return union > 0 ? inter / union : 0;
}

// ── Detector (BlazeFace) ──────────────────────────────────────────────────────
// API de blazeface:
//   const predictions = await detector.estimateFaces(videoEl, false);
//   predictions[i].topLeft      = [x, y]       (Float32Array)
//   predictions[i].bottomRight  = [x, y]       (Float32Array)
//   predictions[i].probability  = [confidence] (Float32Array)
async function loadDetector() {
  statusEl.textContent = 'Cargando modelo BlazeFace…';
  log('Cargando modelo BlazeFace...');
  detector = await blazeface.load();
  log('Modelo BlazeFace cargado OK.');
  statusEl.textContent = 'Modelo cargado.';
}

// ── Cámara ────────────────────────────────────────────────────────────────────
async function startCamera() {
  log('Solicitando permiso de cámara...');
  stream = await navigator.mediaDevices.getUserMedia({
    video: { width: { ideal: 640 }, height: { ideal: 480 }, facingMode: { ideal: 'user' } },
    audio: false,
  });
  videoEl.srcObject = stream;
  await new Promise(r => (videoEl.onloadedmetadata = r));
  await videoEl.play();
  canvasEl.width  = videoEl.videoWidth;
  canvasEl.height = videoEl.videoHeight;
  log(`Cámara activa: ${videoEl.videoWidth}x${videoEl.videoHeight}`);
}

// ── SSE ───────────────────────────────────────────────────────────────────────
function openSSE() {
  sessionId = uuid();
  sessionEl.textContent = sessionId;
  const url = `${API2}/realtime/stream/${sessionId}`;
  log(`Abriendo SSE → ${url}`);
  sse = new EventSource(url);

  sse.onopen = () => log('SSE conectado OK.');

  sse.onmessage = (ev) => {
    logStats.sseReceived++;
    try {
      const data = JSON.parse(ev.data);
      log(`SSE msg recibido → token=${data.face_token?.slice(0,8)}… menor=${data.is_minor} edad=${data.estimated_age} conf=${data.confidence}`);
      for (const face of tracked.values()) {
        if (face.token === data.face_token) {
          face.status = 'classified';
          face.age = data.estimated_age;
          face.isMinor = !!data.is_minor;
          face.confidence = data.confidence;
          log(`  Clasificado: ${face.id} = ${face.isMinor ? 'MENOR' : 'adulto'} (${face.age} años)`);
          renderFaceList();
          break;
        }
      }
    } catch (e) { /* heartbeat */ }
  };

  sse.onerror = () => {
    log(`SSE error/reconectando (readyState=${sse?.readyState})`);
    statusEl.textContent = 'SSE reconectando…';
  };
}

// ── Envío al backend ───────────────────────────────────────────────────────────
async function sendFaceToBackend(face) {
  const { x, y, w, h } = face.bbox;
  const pad = Math.round(0.1 * Math.max(w, h));
  const cx = Math.max(0, x - pad);
  const cy = Math.max(0, y - pad);
  const cw = Math.min(canvasEl.width  - cx, w + pad * 2);
  const ch = Math.min(canvasEl.height - cy, h + pad * 2);
  if (cw <= 0 || ch <= 0) {
    log(`WARN ${face.id}: crop inválido cw=${cw} ch=${ch}`);
    return;
  }

  cropCanvas.width  = cw;
  cropCanvas.height = ch;
  cropCtx.drawImage(videoEl, cx, cy, cw, ch, 0, 0, cw, ch);
  const blob = await new Promise(r => cropCanvas.toBlob(r, 'image/jpeg', CROP_JPEG_QUALITY));
  if (!blob) { log(`WARN ${face.id}: blob nulo`); return; }

  const form = new FormData();
  form.append('session_id', sessionId);
  form.append('face_token', face.token);
  form.append('image', blob, `${face.id}.jpg`);

  face.status = 'pending';
  face.inflightTs = performance.now();
  logStats.newFacesSent++;
  log(`POST /realtime/faces → ${face.id} token=${face.token.slice(0,8)}… blob=${blob.size}B`);
  renderFaceList();

  try {
    const res = await fetch(`${API1}/realtime/faces`, { method: 'POST', body: form });
    if (res.ok) {
      logStats.apiOk++;
      log(`  API1 → 202 OK para ${face.id}, esperando SSE...`);
    } else {
      logStats.apiErr++;
      const detail = await res.text().catch(() => '?');
      const msg = `API1 ${res.status} para ${face.id}: ${detail}`;
      logStats.errors.push(msg);
      log(`  ERROR: ${msg}`);
      face.status = 'error';
      renderFaceList();
    }
  } catch (e) {
    logStats.apiErr++;
    const msg = `fetch error ${face.id}: ${e.message}`;
    logStats.errors.push(msg);
    log(`  ERROR: ${msg}`);
    face.status = 'error';
    renderFaceList();
  }
}

// ── Tracking ──────────────────────────────────────────────────────────────────
// Recibe bboxes ya normalizadas: [{x, y, w, h}]
function matchAndTrack(currBoxes) {

  const used = new Set();
  const nowFrame = frameCount;

  // 1) Emparejar caras ya tracked con la detección actual por IoU
  for (const [id, face] of tracked) {
    let bestIdx = -1, bestScore = IOU_MATCH_THRESHOLD;
    currBoxes.forEach((bb, i) => {
      if (used.has(i)) return;
      const score = iou(face.bbox, bb);
      if (score > bestScore) { bestScore = score; bestIdx = i; }
    });
    if (bestIdx >= 0) {
      face.bbox = currBoxes[bestIdx];
      face.lastSeen = nowFrame;
      face.confirmedFrames = (face.confirmedFrames || 0) + 1;
      used.add(bestIdx);
    }
  }

  // 2) Detecciones sin match → cara nueva (aún no confirmada)
  currBoxes.forEach((bb, i) => {
    if (used.has(i)) return;
    const id = `cara${nextFaceNum++}`;
    const face = { id, bbox: bb, lastSeen: nowFrame, token: uuid(),
      status: 'new', isMinor: null, age: null, confidence: null, inflightTs: 0,
      confirmedFrames: 1 };
    tracked.set(id, face);
    log(`Nueva cara candidata: ${id} bbox=(${Math.round(bb.x)},${Math.round(bb.y)},${Math.round(bb.w)}x${Math.round(bb.h)}) [1/${MIN_CONFIRM_FRAMES} frames]`);
  });

  // 3) Confirmar caras nuevas que llevan suficientes frames seguidos
  for (const face of tracked.values()) {
    if (face.status === 'new' && face.confirmedFrames >= MIN_CONFIRM_FRAMES) {
      log(`Cara confirmada: ${face.id} (${face.confirmedFrames} frames) → enviando al backend`);
      sendFaceToBackend(face);
    }
  }

  // 4) Descartar caras perdidas
  for (const [id, face] of tracked) {
    if (nowFrame - face.lastSeen > TRACK_LOST_FRAMES) {
      log(`Cara perdida del tracking: ${id}`);
      tracked.delete(id);
    }
  }

  // 5) Reintentar pendientes sin respuesta
  const now = performance.now();
  for (const face of tracked.values()) {
    if (face.status === 'pending' && now - face.inflightTs > RESEND_TIMEOUT_MS) {
      log(`Reintento ${face.id} (${Math.round((now - face.inflightTs)/1000)}s sin SSE)`);
      sendFaceToBackend(face);
    }
  }
}

// ── Render ────────────────────────────────────────────────────────────────────
function pixelateRegion(x, y, w, h) {
  x = Math.max(0, Math.floor(x));
  y = Math.max(0, Math.floor(y));
  w = Math.min(canvasEl.width  - x, Math.floor(w));
  h = Math.min(canvasEl.height - y, Math.floor(h));
  if (w <= 0 || h <= 0) return;
  const blockW = Math.max(4, Math.round(w / PIXEL_BLOCK));
  const blockH = Math.max(4, Math.round(h / PIXEL_BLOCK));
  const prev = ctx.imageSmoothingEnabled;
  ctx.imageSmoothingEnabled = false;
  cropCanvas.width  = blockW;
  cropCanvas.height = blockH;
  cropCtx.imageSmoothingEnabled = false;
  cropCtx.drawImage(canvasEl, x, y, w, h, 0, 0, blockW, blockH);
  ctx.drawImage(cropCanvas, 0, 0, blockW, blockH, x, y, w, h);
  ctx.imageSmoothingEnabled = prev;
}

function drawOverlays() {
  ctx.drawImage(videoEl, 0, 0, canvasEl.width, canvasEl.height);

  // Solo actuar sobre caras vistas recientemente Y confirmadas (evita cajas/pixelado fantasma)
  const visibleFaces = [...tracked.values()].filter(
    f => frameCount - f.lastSeen <= VISIBLE_FRAMES && f.confirmedFrames >= MIN_CONFIRM_FRAMES
  );

  // Pixelar menores primero (debajo de los bordes)
  for (const face of visibleFaces) {
    if (face.isMinor === true) pixelateRegion(face.bbox.x, face.bbox.y, face.bbox.w, face.bbox.h);
  }

  // Bordes y etiquetas
  ctx.lineWidth = 2;
  ctx.font = '13px -apple-system, sans-serif';
  ctx.textBaseline = 'top';
  for (const face of visibleFaces) {
    let color = '#888';
    if (face.status === 'pending' || face.status === 'new') color = '#7A7773';
    else if (face.status === 'error')   color = '#FF6600';
    else if (face.isMinor === true)     color = '#B43C28';
    else if (face.isMinor === false)    color = '#3E6B4E';

    ctx.strokeStyle = color;
    ctx.strokeRect(face.bbox.x, face.bbox.y, face.bbox.w, face.bbox.h);

    let label = face.id;
    if (face.status === 'pending' || face.status === 'new') label += ' · analizando…';
    else if (face.status === 'error')   label += ' · error backend';
    else if (face.isMinor === true)     label += ` · MENOR (${face.age}a, ${confLabel(face.confidence)})`;
    else if (face.isMinor === false)    label += ` · adulto (${face.age}a, ${confLabel(face.confidence)})`;

    const padX = 5, padY = 3;
    const textW = ctx.measureText(label).width;
    const ly = Math.max(0, face.bbox.y - 20);
    ctx.fillStyle = 'rgba(0,0,0,.55)';
    ctx.fillRect(face.bbox.x, ly, textW + padX * 2, 18);
    ctx.fillStyle = '#fff';
    ctx.fillText(label, face.bbox.x + padX, ly + padY);
  }
}

function renderFaceList() {
  const items = [...tracked.values()];
  if (items.length === 0) {
    faceListEl.innerHTML = '<div style="font-size:12px; color:var(--muted)">Ninguna aún.</div>';
  } else {
    faceListEl.innerHTML = items.map(f => {
      let cls = 'pending', verdict = 'Analizando…';
      if (f.status === 'error')        { verdict = '⚠ Error al enviar al backend'; }
      if (f.isMinor === true)          { cls = 'minor'; verdict = `MENOR · ${f.age} años · ${confLabel(f.confidence)}`; }
      else if (f.isMinor === false)    { cls = 'adult'; verdict = `Adulto · ${f.age} años · ${confLabel(f.confidence)}`; }
      return `<div class="face-card ${cls}"><div class="id">${f.id}</div><div class="verdict">${verdict}</div></div>`;
    }).join('');
  }

  const total   = items.length;
  const minors  = items.filter(f => f.isMinor === true).length;
  const adults  = items.filter(f => f.isMinor === false).length;
  const pending = items.filter(f => f.status === 'pending' || f.status === 'new').length;
  statsEl.innerHTML = `
    Caras activas: <strong>${total}</strong><br>
    · Menores pixelados: <strong>${minors}</strong><br>
    · Adultos: <strong>${adults}</strong><br>
    · Analizando: <strong>${pending}</strong><br>
    FPS: <strong>${fps.toFixed(1)}</strong>
  `;
}

// ── Loop principal ─────────────────────────────────────────────────────────────
async function loop() {
  if (!running) return;
  try {
    // annotateBoxes=true para obtener probability y poder filtrar falsos positivos
    const detections = await detector.estimateFaces(videoEl, false, false, true);
    logStats.framesProcessed++;

    // Filtrar por confianza y tamaño mínimo
    const valid = detections.filter(d => {
      const prob = Array.isArray(d.probability) ? d.probability[0] : (d.probability ?? 1);
      return prob >= MIN_FACE_PROBABILITY;
    });
    logStats.detectedSum += valid.length;

    // Primera vez con cara válida: logear estructura raw (solo una vez por sesión)
    if (valid.length > 0 && !rawLogDone) {
      rawLogDone = true;
      const d = valid[0];
      const prob = Array.isArray(d.probability) ? d.probability[0] : d.probability;
      log(`Raw detection[0]: topLeft=[${Array.from(d.topLeft).map(v=>v.toFixed(1))}] bottomRight=[${Array.from(d.bottomRight).map(v=>v.toFixed(1))}] prob=${prob?.toFixed(3)}`);
    }

    // Convertir BlazeFace → formato interno {x,y,w,h}
    const normalized = valid.map(d => {
      const tl = d.topLeft;
      const br = d.bottomRight;
      return {
        x: tl[0], y: tl[1],
        w: br[0] - tl[0],
        h: br[1] - tl[1],
      };
    }).filter(b => b.w > 20 && b.h > 20); // mínimo 20px de lado

    matchAndTrack(normalized);
  } catch (e) {
    if (frameCount < 5) log(`Error estimateFaces (frame ${frameCount}): ${e.message}`);
  }
  drawOverlays();

  frameCount++;
  const now = performance.now();
  if (now - lastFpsTime > 500) {
    fps = (frameCount * 1000) / (now - lastFpsTime);
    frameCount = 0;
    lastFpsTime = now;
    renderFaceList();
  }

  if (now - lastLogTime > 1000) printSecondLog();

  rafId = requestAnimationFrame(loop);
}

// ── Inicio / parada ────────────────────────────────────────────────────────────
async function start() {
  btnStart.disabled = true;
  try {
    if (!detector) await loadDetector();
    await startCamera();
    openSSE();
    running = true;
    frameCount = 0;
    lastLogTime = performance.now();
    logStats = { framesProcessed: 0, detectedSum: 0, newFacesSent: 0, apiOk: 0, apiErr: 0, sseReceived: 0, errors: [] };
    btnStop.disabled = false;
    statusEl.textContent = 'En directo.';
    log('=== Loop iniciado ===');
    loop();
  } catch (e) {
    log(`ERROR al iniciar: ${e.message}`);
    statusEl.textContent = `Error: ${e.message}`;
    btnStart.disabled = false;
  }
}

function stop() {
  running = false;
  if (rafId) cancelAnimationFrame(rafId);
  if (stream) stream.getTracks().forEach(t => t.stop());
  if (sse) sse.close();
  sse = null; stream = null;
  tracked.clear();
  nextFaceNum = 1;
  rawLogDone = false;
  ctx.clearRect(0, 0, canvasEl.width, canvasEl.height);
  statusEl.textContent = 'Detenido.';
  sessionEl.textContent = '—';
  btnStart.disabled = false;
  btnStop.disabled = true;
  log('=== Loop detenido ===');
  renderFaceList();
}

btnStart.addEventListener('click', start);
btnStop.addEventListener('click', stop);
window.addEventListener('beforeunload', stop);
