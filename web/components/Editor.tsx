'use client'

import { useEffect, useRef } from 'react'
import * as Y from 'yjs'
import { WebsocketProvider } from 'y-websocket'
import { yCollab } from 'y-codemirror.next'
import { EditorState } from '@codemirror/state'
import { EditorView } from '@codemirror/view'
import { basicSetup } from 'codemirror'
import { javascript } from '@codemirror/lang-javascript'

const COLORS = ['#e06c75', '#61afef', '#98c379', '#e5c07b', '#c678dd', '#56b6c2']

export default function Editor({ roomId }: { roomId: string }) {
  const host = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!host.current) return

    const ydoc = new Y.Doc()
    const raw = new URLSearchParams(window.location.search).get('port')
   const port = raw && /^\d{2,5}$/.test(raw) ? raw : '4100' // digits only, never trust a URL param
    const provider = new WebsocketProvider(`ws://localhost:${port}/ws`, roomId, ydoc)
    const ytext = ydoc.getText('code')

    // Presence: who am I and what colour is my cursor
    const color = COLORS[Math.floor(Math.random() * COLORS.length)]
    provider.awareness.setLocalStateField('user', {
      name: `Guest ${Math.floor(Math.random() * 1000)}`,
      color,
      colorLight: color + '33',
    })

    const view = new EditorView({
      parent: host.current,
      state: EditorState.create({
        doc: ytext.toString(),
        extensions: [
          basicSetup,
          javascript({ typescript: true }),
          yCollab(ytext, provider.awareness),
        ],
      }),
    })

    return () => {
      view.destroy()
      provider.destroy()
      ydoc.destroy()
    }
  }, [roomId])

  return <div ref={host} className="h-[70vh] overflow-auto rounded-lg border" />
}