#!/usr/bin/env node
/**
 * Export HubSpot note comments for every ticket in a pipeline.
 *
 *   1. Find the ticket pipeline (default label "Customer Support")
 *   2. Fetch all tickets in that pipeline            (public CRM API, private app token)
 *   3. Fetch notes associated with each ticket       (public CRM API, private app token)
 *   4. Fetch comments on each note                   (internal commenting API, browser session cookie)
 *   5. Write one CSV row per comment
 *   6. Download each note's attachments into ATTACHMENTS_DIR/<noteId>/    (Files API, private app token)
 *
 * Required env vars:
 *   HUBSPOT_TOKEN      Private app token (scopes: tickets, crm.objects.contacts.read for notes,
 *                      files + files.ui_hidden.read for attachments)
 *   HUBSPOT_COOKIE     Full "cookie" header value copied from a logged-in browser request
 *   HUBSPOT_CSRF       Value of the "x-hubspot-csrf-hubspotapi" header from the same request
 *
 * Optional env vars:
 *   HUBSPOT_PORTAL_ID  default 50231172
 *   PIPELINE_LABEL     default "Customer Support"
 *   OUTPUT_FILE        default ./ticket_note_comments.csv
 *   CONCURRENCY        default 5 (parallel comment requests)
 *   INCLUDE_EMPTY      "1" to also write a row for notes that have no comments
 *   DEBUG              "1" to print the raw listComments response for the first note
 *   ATTACHMENTS_DIR    default ./attachments
 *   SKIP_ATTACHMENTS   "1" to skip downloading note attachments
 *   TICKET_ID          export only this ticket (PIPELINE_LABEL is ignored), handy for testing
 *   NO_NOTES_FILE      default ./tickets_without_notes.csv (tickets that have no notes)
 */

const fs = require('fs');
const path = require('path');

const TOKEN = process.env.HUBSPOT_TOKEN;
const COOKIE = process.env.HUBSPOT_COOKIE;
const CSRF = process.env.HUBSPOT_CSRF;
const PORTAL_ID = process.env.HUBSPOT_PORTAL_ID || '8595997';
const PIPELINE_LABEL = (process.env.PIPELINE_LABEL || 'USA Aftersales_old').trim();
const OUTPUT_FILE = path.resolve(process.env.OUTPUT_FILE || 'ticket_note_comments.csv');
const CONCURRENCY = Number(process.env.CONCURRENCY || 5);
const INCLUDE_EMPTY = process.env.INCLUDE_EMPTY === '1';
const DEBUG = process.env.DEBUG === '1';
const ATTACHMENTS_DIR = path.resolve(process.env.ATTACHMENTS_DIR || 'attachments');
const SKIP_ATTACHMENTS = process.env.SKIP_ATTACHMENTS === '1';
const TICKET_ID = (process.env.TICKET_ID || '').trim();
const OWNERS_FILE = path.resolve(process.env.OWNERS_FILE || 'owners_users.csv');
const NO_NOTES_FILE = path.resolve(process.env.NO_NOTES_FILE || 'tickets_without_notes.csv');

const API = 'https://api.hubapi.com';
const COMMENTS_URL =
  `https://app.hubspot.com/api/chirp-frontend-app/v1/gateway/com.hubspot.commenting.rpc.CommentingV3Rpc/listComments` +
  `?portalId=${PORTAL_ID}&clienttimeout=5000&hs_static_app=crm-records-ui`;

for (const [name, val] of Object.entries({ HUBSPOT_TOKEN: TOKEN, HUBSPOT_COOKIE: COOKIE, HUBSPOT_CSRF: CSRF })) {
  if (!val) {
    console.error(`Missing env var ${name}`);
    process.exit(1);
  }
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

// fetch with retry on 429 / 5xx
async function request(url, options = {}, attempt = 1) {
  const res = await fetch(url, options);
  if ((res.status === 429 || res.status >= 500) && attempt <= 5) {
    const wait = Number(res.headers.get('retry-after')) * 1000 || 1000 * 2 ** attempt;
    console.warn(`  ${res.status} on ${url.split('?')[0]}, retrying in ${wait}ms`);
    await sleep(wait);
    return request(url, options, attempt + 1);
  }
  const text = await res.text();
  if (!res.ok) throw new Error(`${res.status} ${res.statusText} ${url}\n${text.slice(0, 500)}`);
  return text ? JSON.parse(text) : {};
}

function hs(pathname, { method = 'GET', body } = {}) {
  return request(`${API}${pathname}`, {
    method,
    headers: { Authorization: `Bearer ${TOKEN}`, 'Content-Type': 'application/json' },
    body: body ? JSON.stringify(body) : undefined,
  });
}

// ---------- 1. Pipeline ----------
async function getPipelineId() {
  const { results } = await hs('/crm/v3/pipelines/tickets');
  const pipeline = results.find((p) => p.label.trim().toLowerCase() === PIPELINE_LABEL.toLowerCase());
  if (!pipeline) {
    throw new Error(
      `Pipeline "${PIPELINE_LABEL}" not found. Available: ${results.map((p) => `"${p.label}" (${p.id})`).join(', ')}`
    );
  }
  return pipeline.id;
}

// ---------- 2. Tickets ----------
async function getTickets(pipelineId) {
  const tickets = [];
  let after;
  do {
    const data = await hs('/crm/v3/objects/tickets/search', {
      method: 'POST',
      body: {
        filterGroups: [{ filters: [{ propertyName: 'hs_pipeline', operator: 'EQ', value: pipelineId }] }],
        properties: ['subject', 'hs_pipeline_stage', 'createdate'],
        sorts: [{ propertyName: 'hs_object_id', direction: 'ASCENDING' }],
        limit: 100,
        after,
      },
    });
    tickets.push(...data.results);
    after = data.paging?.next?.after;
    await sleep(250); // search API is limited to ~4 req/s
  } while (after);
  return tickets;
}

// ---------- 3. Notes per ticket ----------
async function getNoteIdsByTicket(ticketIds) {
  const map = new Map(ticketIds.map((id) => [id, []]));
  for (let i = 0; i < ticketIds.length; i += 1000) {
    const chunk = ticketIds.slice(i, i + 1000);
    const data = await hs('/crm/v4/associations/tickets/notes/batch/read', {
      method: 'POST',
      body: { inputs: chunk.map((id) => ({ id })) },
    });
    for (const row of data.results || []) {
      map.get(String(row.from.id)).push(...row.to.map((t) => String(t.toObjectId)));
    }
  }
  return map;
}

// ---------- 4. Comments per note ----------
// browser session headers for internal app APIs (comments, sensitive file downloads)
const SESSION_HEADERS = {
  cookie: COOKIE,
  referer: `https://app.hubspot.com/contacts/${PORTAL_ID}/`,
  'x-hubspot-csrf-hubspotapi': CSRF,
  'user-agent':
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36',
};

let debugPrinted = false;

async function getComments(noteId) {
  const data = await request(COMMENTS_URL, {
    method: 'POST',
    headers: {
      ...SESSION_HEADERS,
      accept: 'application/json, text/javascript, */*; q=0.01',
      'content-type': 'application/json',
      origin: 'https://app.hubspot.com',
    },
    body: JSON.stringify({ filters: { objectType: 'ENGAGEMENT', objectId: noteId }, sortDescending: true }),
  });

  if (DEBUG && !debugPrinted) {
    debugPrinted = true;
    console.log(`\nRaw listComments response for note ${noteId}:\n${JSON.stringify(data, null, 2)}\n`);
  }

  // Internal API, so the shape is not documented; accept the common variants
  const list = data.comments || data.results || data.data?.comments || (Array.isArray(data) ? data : []);
  return list.map(normalizeComment);
}

const pick = (obj, ...keys) => {
  for (const k of keys) {
    const v = k.split('.').reduce((o, p) => (o == null ? undefined : o[p]), obj);
    if (v !== undefined && v !== null && v !== '') return v;
  }
  return '';
};

const stripHtml = (s) =>
  String(s)
    .replace(/<br\s*\/?>/gi, '\n')
    .replace(/<\/p>/gi, '\n')
    .replace(/<[^>]+>/g, '')
    .replace(/&nbsp;/g, ' ')
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .trim();

// Pulls HubSpot file ids and plain external image/link URLs out of a comment body + its attachmentIds
function extractCommentFiles(c) {
  const html = String(pick(c, 'body', 'text', 'message', 'content', 'commentBody'));
  const fileIds = new Set((Array.isArray(c.attachmentIds) ? c.attachmentIds : []).map(String));
  const urls = new Set();
  for (const m of html.matchAll(/(?:src|href)\s*=\s*"([^"]+)"/gi)) {
    const url = m[1].replace(/&amp;/g, '&');
    const idMatch = url.match(/\/files\/(?:v\d\/)?(\d+)(?:\/|\?|$)/);
    if (idMatch && /hubspot|hubapi/i.test(url)) fileIds.add(idMatch[1]);
    else if (/^https?:\/\//i.test(url) && /\.(png|jpe?g|gif|webp|bmp|svg|pdf|docx?|xlsx?|pptx?|csv|txt|zip|mp4|mov)(\?|$)/i.test(url)) urls.add(url);
    else if (/^https?:\/\/[^"]*\.(hubspotusercontent|hubfs)/i.test(url)) urls.add(url);
  }
  return { fileIds: [...fileIds], urls: [...urls] };
}

function normalizeComment(c) {
  const ts = pick(c, 'createdAt', 'timestamp', 'created', 'createdTimestamp');
  return {
    files: extractCommentFiles(c),
    userId: String(pick(c, 'userId', 'author.id', 'createdBy.id', 'createdBy') || ''),
    id: pick(c, 'id', 'commentId'),
    author: pick(c, 'author.email', 'author.name', 'createdBy.email', 'createdByUserEmail', 'userEmail', 'createdBy', 'userId'),
    createdAt: ts && !isNaN(Number(ts)) ? new Date(Number(ts)).toISOString() : ts,
    text: stripHtml(pick(c, 'body', 'text', 'message', 'content', 'commentBody')),
  };
}

// ---------- 4b. Attachments per note ----------
async function getAttachmentIdsByNote(noteIds) {
  const map = new Map();
  for (let i = 0; i < noteIds.length; i += 100) {
    const data = await hs('/crm/v3/objects/notes/batch/read', {
      method: 'POST',
      body: { properties: ['hs_attachment_ids'], inputs: noteIds.slice(i, i + 100).map((id) => ({ id })) },
    });
    for (const note of data.results || []) {
      const ids = (note.properties.hs_attachment_ids || '').split(';').map((s) => s.trim()).filter(Boolean);
      if (ids.length) map.set(String(note.id), ids);
    }
  }
  return map;
}

// ---------- Owners / users ----------
// Fetches all owners (active + archived), writes them to OWNERS_FILE and returns a lookup by owner id / user id
async function getOwners() {
  const owners = [];
  for (const archived of [false, true]) {
    let after;
    do {
      const qs = new URLSearchParams({ limit: '500', archived: String(archived) });
      if (after) qs.set('after', after);
      const data = await hs(`/crm/v3/owners?${qs}`);
      owners.push(...(data.results || []));
      after = data.paging?.next?.after;
    } while (after);
  }

  const byId = new Map();
  const out = fs.createWriteStream(OWNERS_FILE);
  out.write(csvRow(['owner_id', 'user_id', 'user_id_including_inactive', 'email', 'first_name', 'last_name', 'full_name', 'type', 'archived']));
  for (const o of owners) {
    const fullName = [o.firstName, o.lastName].filter(Boolean).join(' ').trim() || o.email || '';
    out.write(csvRow([o.id, o.userId ?? '', o.userIdIncludingInactive ?? '', o.email || '', o.firstName || '', o.lastName || '', fullName, o.type || '', o.archived]));
    for (const key of [o.id, o.userId, o.userIdIncludingInactive]) {
      if (key != null && key !== '' && !byId.has(String(key))) byId.set(String(key), fullName);
    }
  }
  await new Promise((r) => out.end(r));
  console.log(`Owners/users: ${owners.length} written to ${OWNERS_FILE}`);
  return byId;
}

// Note body (hs_note_body) per note id, as plain text
async function getNoteBodies(noteIds) {
  const map = new Map();
  for (let i = 0; i < noteIds.length; i += 100) {
    const data = await hs('/crm/v3/objects/notes/batch/read', {
      method: 'POST',
      body: {
        properties: ['hs_note_body', 'hs_created_by_user_id', 'hubspot_owner_id', 'hs_createdate'],
        inputs: noteIds.slice(i, i + 100).map((id) => ({ id })),
      },
    });
    for (const note of data.results || []) {
      const p = note.properties;
      map.set(String(note.id), {
        body: stripHtml(p.hs_note_body || ''),
        // creator user id, falling back to the note owner
        authorId: String(p.hs_created_by_user_id || p.hubspot_owner_id || ''),
        createdAt: p.hs_createdate || '',
      });
    }
  }
  return map;
}

const safeName = (s) => String(s).replace(/[\/\\:*?"<>|\x00-\x1f]/g, '_').trim().slice(0, 180) || 'file';

// fetch a binary file with retry on 429 / 5xx and write it to disk
async function download(url, dest, headers = {}, attempt = 1) {
  const res = await fetch(url, { headers });
  if ((res.status === 429 || res.status >= 500) && attempt <= 5) {
    await sleep(1000 * 2 ** attempt);
    return download(url, dest, headers, attempt + 1);
  }
  if (!res.ok) throw new Error(`${res.status} ${res.statusText} downloading file`);
  await fs.promises.writeFile(dest, Buffer.from(await res.arrayBuffer()));
}

// Downloads every attachment of one note into ATTACHMENTS_DIR/<noteId>/, skipping files already on disk
async function downloadNoteAttachments(noteId, fileIds) {
  const dir = path.join(ATTACHMENTS_DIR, noteId);
  await fs.promises.mkdir(dir, { recursive: true });
  const used = new Set();
  let saved = 0;
  for (const fileId of fileIds) {
    const meta = await hs(`/files/v3/files/${fileId}`);
    let name = safeName(meta.extension ? `${meta.name}.${meta.extension}` : meta.name || fileId);
    if (used.has(name)) name = `${fileId}_${name}`;
    used.add(name);
    const dest = path.join(dir, name);
    if (fs.existsSync(dest)) continue;
    // Sensitive files can't get a signed URL with a private app token, so fetch them through
    // HubSpot's file proxy ("url") with the browser session instead
    if (meta.access === 'HIDDEN_SENSITIVE') {
      await download(meta.url, dest, SESSION_HEADERS);
    } else {
      const { url } = await hs(`/files/v3/files/${fileId}/signed-url`);
      await download(url, dest);
    }
    saved++;
  }
  return saved;
}

// Downloads files found in comments (body images/links + attachments) into the same ATTACHMENTS_DIR/<noteId>/
// folder, with a "comment<id>_" prefix so they are told apart from the note's own attachments
async function downloadCommentFiles(noteId, commentFiles) {
  const dir = path.join(ATTACHMENTS_DIR, noteId);
  await fs.promises.mkdir(dir, { recursive: true });
  let saved = 0;
  for (const { commentId, fileIds, urls } of commentFiles) {
    const prefix = `comment${commentId}_`;
    for (const fileId of fileIds) {
      const meta = await hs(`/files/v3/files/${fileId}`);
      const name = prefix + safeName(meta.extension ? `${meta.name}.${meta.extension}` : meta.name || fileId);
      const dest = path.join(dir, name);
      if (fs.existsSync(dest)) continue;
      if (meta.access === 'HIDDEN_SENSITIVE') {
        await download(meta.url, dest, SESSION_HEADERS);
      } else {
        const { url } = await hs(`/files/v3/files/${fileId}/signed-url`);
        await download(url, dest);
      }
      saved++;
    }
    for (const url of urls) {
      const base = decodeURIComponent(new URL(url).pathname.split('/').pop() || 'file');
      const dest = path.join(dir, prefix + safeName(base));
      if (fs.existsSync(dest)) continue;
      await download(url, dest);
      saved++;
    }
  }
  return saved;
}

// ---------- 5. CSV ----------
const csvCell = (v) => {
  const s = typeof v === 'object' ? JSON.stringify(v) : String(v ?? '');
  return /[",\r\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
};
const csvRow = (cells) => cells.map(csvCell).join(',') + '\n';

// simple promise pool
async function mapPool(items, limit, fn) {
  let i = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (i < items.length) {
      const idx = i++;
      await fn(items[idx], idx);
    }
  });
  await Promise.all(workers);
}

async function main() {
  let tickets;
  if (TICKET_ID) {
    console.log(`Fetching single ticket ${TICKET_ID}...`);
    tickets = [await hs(`/crm/v3/objects/tickets/${TICKET_ID}?properties=subject,hs_pipeline_stage,createdate`)];
  } else {
    console.log(`Looking up pipeline "${PIPELINE_LABEL}"...`);
    const pipelineId = await getPipelineId();
    console.log(`Pipeline id: ${pipelineId}`);
    tickets = await getTickets(pipelineId);
  }
  console.log(`Tickets found: ${tickets.length}`);

  const notesByTicket = await getNoteIdsByTicket(tickets.map((t) => String(t.id)));
  const jobs = [];
  for (const t of tickets) {
    for (const noteId of notesByTicket.get(String(t.id)) || []) {
      jobs.push({ ticketId: t.id, subject: t.properties.subject || '', noteId });
    }
  }
  console.log(`Notes found: ${jobs.length}`);

  // tickets without any notes go to their own file
  const noNotes = tickets.filter((t) => !(notesByTicket.get(String(t.id)) || []).length);
  const noNotesOut = fs.createWriteStream(NO_NOTES_FILE);
  noNotesOut.write(csvRow(['ticket_id', 'ticket_subject', 'pipeline_stage', 'created_at']));
  for (const t of noNotes) {
    noNotesOut.write(csvRow([t.id, t.properties.subject || '', t.properties.hs_pipeline_stage || '', t.properties.createdate || '']));
  }
  await new Promise((r) => noNotesOut.end(r));
  console.log(`Tickets without notes: ${noNotes.length} written to ${NO_NOTES_FILE}`);

  const noteBodies = await getNoteBodies([...new Set(jobs.map((j) => j.noteId))]);
  const ownerNames = await getOwners();
  // name from the owners/users list by user id; fall back to the raw id when no match
  const authorName = (c) => ownerNames.get(c.userId) || c.userId || c.author;

  const out = fs.createWriteStream(OUTPUT_FILE);
  out.write(
    csvRow(['ticket_id', 'ticket_subject', 'note_id', 'note_body', 'note_author_id', 'note_author', 'note_created_at', 'comment_id', 'comment_author_id', 'comment_author', 'comment_created_at', 'comment_text'])
  );

  let commentCount = 0;
  let failed = 0;
  const commentFilesByNote = new Map();
  await mapPool(jobs, CONCURRENCY, async (job, idx) => {
    try {
      const comments = await getComments(job.noteId);
      const n = noteBodies.get(job.noteId) || { body: '', authorId: '', createdAt: '' };
      const noteCols = [n.body, n.authorId, ownerNames.get(n.authorId) || n.authorId, n.createdAt];
      if (!comments.length && INCLUDE_EMPTY) out.write(csvRow([job.ticketId, job.subject, job.noteId, ...noteCols, '', '', '', '', '']));
      for (const c of comments) {
        out.write(csvRow([job.ticketId, job.subject, job.noteId, ...noteCols, c.id, c.userId, authorName(c), c.createdAt, c.text]));
        if (c.files.fileIds.length || c.files.urls.length) {
          if (!commentFilesByNote.has(job.noteId)) commentFilesByNote.set(job.noteId, []);
          commentFilesByNote.get(job.noteId).push({ commentId: c.id, ...c.files });
        }
      }
      commentCount += comments.length;
    } catch (err) {
      failed++;
      console.error(`  Note ${job.noteId} (ticket ${job.ticketId}) failed: ${err.message.split('\n')[0]}`);
      if (/^(401|403)/.test(err.message)) {
        console.error('  Session cookie / CSRF token looks expired. Copy a fresh request from the browser.');
        process.exit(1);
      }
    }
    if ((idx + 1) % 50 === 0) console.log(`  ${idx + 1}/${jobs.length} notes processed`);
  });

  await new Promise((r) => out.end(r));
  console.log(`\nDone. ${commentCount} comments written to ${OUTPUT_FILE}${failed ? ` (${failed} notes failed)` : ''}`);

  if (SKIP_ATTACHMENTS) return;
  const noteIds = [...new Set(jobs.map((j) => j.noteId))];
  const attachmentsByNote = await getAttachmentIdsByNote(noteIds);
  const fileTotal = [...attachmentsByNote.values()].reduce((n, ids) => n + ids.length, 0);
  console.log(`\nNotes with attachments: ${attachmentsByNote.size} (${fileTotal} files)`);

  let savedCount = 0;
  let attachFailed = 0;
  await mapPool([...attachmentsByNote], CONCURRENCY, async ([noteId, fileIds]) => {
    try {
      savedCount += await downloadNoteAttachments(noteId, fileIds);
    } catch (err) {
      attachFailed++;
      console.error(`  Attachments for note ${noteId} failed: ${err.message.split('\n')[0]}`);
    }
  });
  console.log(
    `Done. ${savedCount} files downloaded to ${ATTACHMENTS_DIR}${attachFailed ? ` (${attachFailed} notes failed)` : ''}`
  );

  const commentFileTotal = [...commentFilesByNote.values()].reduce(
    (n, list) => n + list.reduce((m, f) => m + f.fileIds.length + f.urls.length, 0),
    0
  );
  console.log(`\nNotes with comment files: ${commentFilesByNote.size} (${commentFileTotal} files)`);
  let commentSaved = 0;
  let commentFileFailed = 0;
  await mapPool([...commentFilesByNote], CONCURRENCY, async ([noteId, list]) => {
    try {
      commentSaved += await downloadCommentFiles(noteId, list);
    } catch (err) {
      commentFileFailed++;
      console.error(`  Comment files for note ${noteId} failed: ${err.message.split('\n')[0]}`);
    }
  });
  console.log(
    `Done. ${commentSaved} comment files downloaded to ${ATTACHMENTS_DIR}${commentFileFailed ? ` (${commentFileFailed} notes failed)` : ''}`
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
