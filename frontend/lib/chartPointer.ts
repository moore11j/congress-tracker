export function getSvgLocalPoint(svg: SVGSVGElement, clientX: number, clientY: number) {
  const ctm = svg.getScreenCTM();
  if (!ctm) return null;

  const point = new DOMPoint(clientX, clientY);
  const local = point.matrixTransform(ctm.inverse());

  return { x: local.x, y: local.y };
}
