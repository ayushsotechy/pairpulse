import { Suspense } from 'react'
import Editor from '@/components/Editor'

async function Room({ params }: { params: Promise<{ roomId: string }> }) {
  const { roomId } = await params
  return (
    <>
      <h1 className="mb-4 text-2xl font-semibold">PairPulse · {roomId}</h1>
      <Editor roomId={roomId} />
    </>
  )
}

export default function RoomPage({ params }: { params: Promise<{ roomId: string }> }) {
  return (
    <main className="mx-auto max-w-4xl p-6">
      <Suspense fallback={<p>Loading room…</p>}>
        <Room params={params} />
      </Suspense>
    </main>
  )
}