import Fastify from 'fastify'
import websocket from '@fastify/websocket'
import type { WebSocket } from 'ws'
import * as Y from 'yjs'
import * as syncProtocol from 'y-protocols/sync'
import * as awarenessProtocol from 'y-protocols/awareness'
import * as encoding from 'lib0/encoding'
import * as decoding from 'lib0/decoding'

const MSG_SYNC = 0       // document updates
const MSG_AWARENESS = 1  // presence: cursors, names, colours
const WS_OPEN = 1

type AwarenessChange = { added: number[]; updated: number[]; removed: number[] }

function send(ws: WebSocket, msg: Uint8Array) {
  if (ws.readyState === WS_OPEN) ws.send(msg)
}

class Room {
  readonly doc = new Y.Doc()
  readonly awareness = new awarenessProtocol.Awareness(this.doc)
  // socket -> awareness client IDs that socket owns (needed to clean up on disconnect)
  readonly conns = new Map<WebSocket, Set<number>>()

  constructor(readonly id: string) {
    this.awareness.setLocalState(null) // the server itself has no cursor

    // Any change to the document -> push to every connected socket
    this.doc.on('update', (update: Uint8Array) => {
      const enc = encoding.createEncoder()
      encoding.writeVarUint(enc, MSG_SYNC)
      syncProtocol.writeUpdate(enc, update)
      this.broadcast(encoding.toUint8Array(enc))
    })

    // Any presence change -> push to every connected socket
    this.awareness.on('update', ({ added, updated, removed }: AwarenessChange, origin: unknown) => {
      const owned = this.conns.get(origin as WebSocket)
      if (owned) {
        added.forEach((id) => owned.add(id))
        removed.forEach((id) => owned.delete(id))
      }
      const enc = encoding.createEncoder()
      encoding.writeVarUint(enc, MSG_AWARENESS)
      encoding.writeVarUint8Array(
        enc,
        awarenessProtocol.encodeAwarenessUpdate(this.awareness, added.concat(updated, removed))
      )
      this.broadcast(encoding.toUint8Array(enc))
    })
  }

  broadcast(msg: Uint8Array) {
    for (const ws of this.conns.keys()) send(ws, msg)
  }
}

const rooms = new Map<string, Room>()
function getRoom(id: string) {
  let room = rooms.get(id)
  if (!room) {
    room = new Room(id)
    rooms.set(id, room)
  }
  return room
}

const app = Fastify({ logger: true })
await app.register(websocket)

app.get('/health', async () => ({ ok: true }))

app.get<{ Params: { roomId: string } }>('/ws/:roomId', { websocket: true }, (socket, req) => {
  const room = getRoom(req.params.roomId)
  room.conns.set(socket, new Set())

  socket.on('message', (data: Buffer) => {
    const decoder = decoding.createDecoder(new Uint8Array(data))
    const type = decoding.readVarUint(decoder)

    if (type === MSG_SYNC) {
      const enc = encoding.createEncoder()
      encoding.writeVarUint(enc, MSG_SYNC)
      // applies the client's update to the doc, and writes a reply into `enc` if one is needed
      syncProtocol.readSyncMessage(decoder, enc, room.doc, socket)
      if (encoding.length(enc) > 1) send(socket, encoding.toUint8Array(enc))
    } else if (type === MSG_AWARENESS) {
      awarenessProtocol.applyAwarenessUpdate(room.awareness, decoding.readVarUint8Array(decoder), socket)
    }
  })

  socket.on('close', () => {
    const owned = room.conns.get(socket) ?? new Set<number>()
    room.conns.delete(socket)
    // tell everyone else this person's cursor is gone
    awarenessProtocol.removeAwarenessStates(room.awareness, [...owned], null)
  })

  // Handshake: "here's what I have, send me what I'm missing"
  const enc = encoding.createEncoder()
  encoding.writeVarUint(enc, MSG_SYNC)
  syncProtocol.writeSyncStep1(enc, room.doc)
  send(socket, encoding.toUint8Array(enc))

  // Also show the new client everyone who's already in the room
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
})

await app.listen({ port: 4000, host: '0.0.0.0' })