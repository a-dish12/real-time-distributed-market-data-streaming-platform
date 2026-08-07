/* Lightweight Charts parses colour strings itself and does not understand oklch(), which is
   what every token in the design system is authored in. Anything crossing into the canvas is
   resolved to hex here first. Values that are already hex/rgb/named pass through untouched. */

export function oklchToHex(input: string): string {
  const m = input.trim().match(/^oklch\(\s*([\d.]+%?)\s+([\d.]+)\s+([\d.]+)/i)
  if (!m) return input

  let L = parseFloat(m[1])
  if (m[1].endsWith('%')) L /= 100
  const C = parseFloat(m[2])
  const h = (parseFloat(m[3]) * Math.PI) / 180
  const a = C * Math.cos(h)
  const b = C * Math.sin(h)

  // Oklab -> LMS -> linear sRGB
  const l_ = L + 0.3963377774 * a + 0.2158037573 * b
  const m_ = L - 0.1055613458 * a - 0.0638541728 * b
  const s_ = L - 0.0894841775 * a - 1.291485548 * b
  const l = l_ ** 3
  const mm = m_ ** 3
  const s = s_ ** 3

  const linear = [
    4.0767416621 * l - 3.3077115913 * mm + 0.2309699292 * s,
    -1.2684380046 * l + 2.6097574011 * mm - 0.3413193965 * s,
    -0.0041960863 * l - 0.7034186147 * mm + 1.707614701 * s,
  ]

  const hex = linear.map((c) => {
    const v = c <= 0.0031308 ? 12.92 * c : 1.055 * Math.max(c, 0) ** (1 / 2.4) - 0.055
    const n = Math.round(Math.min(1, Math.max(0, v)) * 255)
    return n.toString(16).padStart(2, '0')
  })
  return `#${hex.join('')}`
}

/** Resolve a CSS custom property to a canvas-safe hex string. */
export function cssColor(name: string): string {
  const raw = getComputedStyle(document.documentElement).getPropertyValue(name).trim()
  return oklchToHex(raw)
}
