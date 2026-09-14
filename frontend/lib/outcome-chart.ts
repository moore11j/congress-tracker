// Presentation only: never use this selection for ledger rows or headline metrics.
export const outcomeChartReturnLimit = 100;

export function selectOutcomeChartPoints<T extends { returnValue: number }>(points: readonly T[]) {
  const visiblePoints = points.filter(
    (point) => Number.isFinite(point.returnValue) && Math.abs(point.returnValue) <= outcomeChartReturnLimit,
  );
  return { points: visiblePoints, omittedCount: points.length - visiblePoints.length };
}
