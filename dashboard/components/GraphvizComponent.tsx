import React from "react"
import { Graphviz } from "@hpcc-js/wasm-graphviz"
import { useEffect, useRef, useState } from "react"

interface GraphvizComponentProps {
  dot: string
  width?: number
  height?: number
}

export default function GraphvizComponent({ dot, width = 1200, height = 800 }: GraphvizComponentProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const [graphviz, setGraphviz] = useState<Graphviz | null>(null)

  // Initialize Graphviz
  useEffect(() => {
    Graphviz.load().then(setGraphviz)
  }, [])

  // Render DOT when either graphviz or dot changes
  useEffect(() => {
    if (!graphviz || !containerRef.current || !dot) return

    try {
      const svg = graphviz.dot(dot)
      containerRef.current.innerHTML = svg
    } catch (error) {
      console.error("Error rendering DOT:", error)
    }
  }, [graphviz, dot])

  return (
    <div 
      ref={containerRef} 
      style={{ 
        width: `${width}px`, 
        height: `${height}px`,
        overflow: 'auto',
        border: '1px solid #e2e8f0',
        borderRadius: '4px',
        padding: '16px'
      }} 
    />
  )
} 