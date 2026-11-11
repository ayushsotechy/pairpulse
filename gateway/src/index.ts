import Fastify from 'fastify'
import websocket from '@fastify/websocket'
import type { WebSocket } from 'ws'
import * as Y from 'yjs'
import * as syncProtocol from 'y-protocols/sync'
import * as awarenessProtocol from 'y-protocols/awareness'
import * as encoding from 'lib0/encoding'
import * as decoding from 'lib0/decoding'
import * as bus from './bus.js'

const MSG_SYNC = 0
const MSG_AWARENESS = 1
const WS_OPEN = 1

type AwarenessChange = { added: number[]; updated: number[]; removed: number[] }

function send(ws: WebSocket, msg: Uint8Array) {
  if (ws.readyState === WS_OPEN) ws.send(msg)
}

// Publish to Redis, but never let a Redis hiccup crash the process
function fire(p: Promise<unknown>) {
  p.catch((err) => console.error('[bus]', err))
}

class Room {
  readonly doc = new Y.Doc()
  readonly awareness = new awarenessProtocol.Awareness(this.doc)
  readonly conns = new Map<WebSocket, Set<number>>()

  constructor(readonly id: string) {
    this.awareness.setLocalState(null)

    this.doc.on('update', (update: Uint8Array, origin: unknown) => {
      const enc = encoding.createEncoder()
      encoding.writeVarUint(enc, MSG_SYNC)
      syncProtocol.writeUpdate(enc, update)
      this.broadcast(encoding.toUint8Array(enc))

      // Only share changes that started on THIS gateway; otherwise gateways echo forever
      if (origin !== bus.REDIS_ORIGIN) fire(bus.publish(this.id, bus.KIND_UPDATE, update))
    })

    this.awareness.on('update', ({ added, updated, removed }: AwarenessChange, origin: unknown) => {
      const owned = this.conns.get(origin as WebSocket)
      if (owned) {
        added.forEach((id) => owned.add(id))
        removed.forEach((id) => owned.delete(id))
      }
      const bytes = awarenessProtocol.encodeAwarenessUpdate(this.awareness, added.concat(updated, removed))
      const enc = encoding.createEncoder()
      encoding.writeVarUint(enc, MSG_AWARENESS)
      encoding.writeVarUint8Array(enc, bytes)
      this.broadcast(encoding.toUint8Array(enc))

      if (origin !== bus.REDIS_ORIGIN) fire(bus.publish(this.id, bus.KIND_AWARENESS, bytes))
    })
  }

  broadcast(msg: Uint8Array) {
    for (const ws of this.conns.keys()) send(ws, msg)
  }
}

async function createRoom(id: string) {
  const room = new Room(id)

  // 1. Subscribe FIRST, so nothing published while we replay can slip through the gap
  await bus.subscribe(id, (kind, payload) => {
    if (kind === bus.KIND_UPDATE) Y.applyUpdate(room.doc, payload, bus.REDIS_ORIGIN)
    else if (kind === bus.KIND_AWARENESS)
      awarenessProtocol.applyAwarenessUpdate(room.awareness, payload, bus.REDIS_ORIGIN)
  })

  // 2. Then rebuild the document from the log (applying an update twice is harmless in a CRDT)
  for (const update of await bus.history(id)) Y.applyUpdate(room.doc, update, bus.REDIS_ORIGIN)

  return room
}

// Store the PROMISE so two sockets joining at once don't create the room twice
const roomPromises = new Map<string, Promise<Room>>()
function getRoom(id: string) {
  let p = roomPromises.get(id)
  if (!p) {
    p = createRoom(id)
    roomPromises.set(id, p)
    p.catch(() => roomPromises.delete(id)) // allow a retry if Redis was down
  }
  return p
}

function join(room: Room, socket: WebSocket) {
  room.conns.set(socket, new Set())

  const enc = encoding.createEncoder()
  encoding.writeVarUint(enc, MSG_SYNC)
  syncProtocol.writeSyncStep1(enc, room.doc)
  send(socket, encoding.toUint8Array(enc))

  const states = room.awareness.getStates()
  if (states.size > 0) {
    const aEnc = encoding.createEncoder()
    encoding.writeVarUint(aEnc, MSG_AWARENESS)
    encoding.writeVarUint8Array(
      aEnc,
      awarenessProtocol.encodeAwarenessUpdate(room.awareness, [...states.keys()])
    )
    send(socket, encoding.toUint8Array(aEnc))
  }
  return room
}

const app = Fastify({ logger: true })
await app.register(websocket)

app.get('/health', async () => ({ ok: true }))

app.get<{ Params: { roomId: string } }>('/ws/:roomId', { websocket: true }, (socket, req) => {
  // Loading a room is now async (it replays Redis). Attach listeners immediately so no
  // message is lost, and make every handler wait for `session` before touching the room.
  const session = getRoom(req.params.roomId).then((room) => join(room, socket))
  session.catch((err) => {
    app.log.error(err)
    socket.close(1011, 'room unavailable')
  })

  socket.on('message', async (data: Buffer) => {
    const room = await session.catch(() => null)
    if (!room) return

    const decoder = decoding.createDecoder(new Uint8Array(data))
    const type = decoding.readVarUint(decoder)

    if (type === MSG_SYNC) {
      const enc = encoding.createEncoder()
      encoding.writeVarUint(enc, MSG_SYNC)
      syncProtocol.readSyncMessage(decoder, enc, room.doc, socket)
      if (encoding.length(enc) > 1) send(socket, encoding.toUint8Array(enc))
    } else if (type === MSG_AWARENESS) {
      awarenessProtocol.applyAwarenessUpdate(room.awareness, decoding.readVarUint8Array(decoder), socket)
    }
  })

  socket.on('close', async () => {
    const room = await session.catch(() => null)
    if (!room) return
    const owned = room.conns.get(socket) ?? new Set<number>()
    room.conns.delete(socket)
    awarenessProtocol.removeAwarenessStates(room.awareness, [...owned], null)
  })
})

   const port = Number(process.env.PORT ?? 4100)
await app.listen({ port, host: '0.0.0.0' })