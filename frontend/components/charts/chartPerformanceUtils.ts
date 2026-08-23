/** Finds the nearest item in a monotonically increasing set of x coordinates. */
export function nearestChartIndex(xValues: readonly number[], target: number) {
  if (xValues.length === 0) return -1;
  let low = 0;
  let high = xValues.length - 1;
  while (low < high) {
    const middle = Math.floor((low + high) / 2);
    if (xValues[middle] < target) low = middle + 1;
    else high = middle;
  }
  const after = low;
  const before = Math.max(0, after - 1);
  return Math.abs(xValues[before] - target) <= Math.abs(xValues[after] - target) ? before : after;
}

export function chartBounds(values: readonly number[]) {
  const finite = values.filter(Number.isFinite);
  const min = Math.min(...finite);
  const max = Math.max(...finite);
  const span = Math.max(max - min, 1);
  const padding = span * 0.14;
  return { min: min - padding, max: max + padding, range: span + padding * 2 };
}
