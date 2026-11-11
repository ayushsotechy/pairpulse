import { Redis } from 'ioredis'
import { randomUUID } from 'node:crypto'
import * as encoding from 'lib0/encoding'
import * as decoding from 'lib0/decoding'

export const NODE_ID = randomUUID() // identifies this gateway process
export const REDIS_ORIGIN = 'redis' // tag for "this change came from another gateway"
export const KIND_UPDATE = 0
export const KIND_AWARENESS = 1

const url = process.env.REDIS_URL ?? 'redis://localhost:6379'
const pub = new Redis(url) // normal commands: publish, xadd, xrange
const sub = new Redis(url) // a connection in subscribe mode can't run other commands

type Handler = (kind: number, payload: Uint8Array) => void
const handlers = new Map<string, Handler>()

const channel = (roomId: string) => `pp:room:${roomId}`
const stream = (roomId: string) => `pp:room:${roomId}:updates`

sub.on('messageBuffer', (ch: Buffer, message: Buffer) => {
  const handler = handlers.get(ch.toString())
  if (!handler) return
  const dec = decoding.createDecoder(new Uint8Array(message))
  const from = decoding.readVarString(dec)
  if (from === NODE_ID) return // ignore our own messages echoing back
  const kind = decoding.readVarUint(dec)
  handler(kind, decoding.readVarUint8Array(dec))
})

export async function subscribe(roomId: string, handler: Handler) {
  handlers.set(channel(roomId), handler)
  await sub.subscribe(channel(roomId))
}

export async function publish(roomId: string, kind: number, payload: Uint8Array) {
  const enc = encoding.createEncoder()
  encoding.writeVarString(enc, NODE_ID)
  encoding.writeVarUint(enc, kind)
  encoding.writeVarUint8Array(enc, payload)

  const p = pub.pipeline()
  // Document updates also go into the durable log. Presence (cursors) does not.
  if (kind === KIND_UPDATE) p.xadd(stream(roomId), '*', 'u', Buffer.from(payload))
  p.publish(channel(roomId), Buffer.from(encoding.toUint8Array(enc)))
  await p.exec()
}

export async function history(roomId: string): Promise<Uint8Array[]> {
  // xrangeBuffer, not xrange: updates are binary and would be corrupted as strings
  const entries = await pub.xrangeBuffer(stream(roomId), '-', '+')
  return entries.map(([, fields]) => new Uint8Array(fields[1]))
}