/** European Black–Scholes estimates; exact terminal cash flows for standard 100-share contracts. */
export type OptionKind = "call" | "put";
export type OptionLeg = { id: string; kind: OptionKind; side: 1 | -1; strike: number; premium: number; quantity: number; source: string; contract?: string };
export type ModelInputs = { spot: number; days: number; volatility: number; rate: number; dividend: number };
export type Position = { legs: OptionLeg[]; shares: number; stockEntry: number; fee: number };
type DatedClose = { price: number; as_of: string; source?: string };
type ListedContract = { ticker: string; kind: OptionKind; strike: number; close?: DatedClose };
export const optionCloseLabel = (close: DatedClose) => `${close.source ?? "Massive"} close · ${close.as_of.slice(0, 10)}`;
export function applyOptionClose(position: Position, close: DatedClose & { ticker: string }): Position {
  return { ...position, legs: position.legs.map(leg => leg.contract === close.ticker && leg.source === "Modeled entry"
    ? { ...leg, premium: close.price, source: optionCloseLabel(close) } : leg) };
}
/** Match all legs together, preserving strike ordering and shared strikes (e.g. straddles). */
export function matchListedPosition(position: Position, contracts: ListedContract[], model: ModelInputs): Position {
  const targets = [...new Set(position.legs.map(l => l.strike))].sort((a, b) => a - b);
  const strikes = [...new Set(contracts.map(c => c.strike))].sort((a, b) => a - b);
  const byKey = new Map(contracts.map(c => [`${c.kind}:${c.strike}`, c]));
  const parents: number[][] = [];
  let costs = strikes.map(() => 0);
  for (let group = 0; group < targets.length; group++) {
    const kinds = position.legs.filter(l => l.strike === targets[group]).map(l => l.kind);
    let best = Infinity, bestIndex = -1;
    const next = strikes.map((strike, index) => {
      if (index > 0 && costs[index - 1] < best) { best = costs[index - 1]; bestIndex = index - 1; }
      (parents[group] ??= [])[index] = bestIndex;
      return kinds.every(kind => byKey.has(`${kind}:${strike}`)) ? (group === 0 ? 0 : best) + Math.abs(strike - targets[group]) : Infinity;
    });
    costs = next;
  }
  let index = costs.indexOf(Math.min(...costs));
  if (!targets.length || index < 0 || !Number.isFinite(costs[index])) return position;
  const chosen = new Map<number, number>();
  for (let group = targets.length - 1; group >= 0; group--) { chosen.set(targets[group], strikes[index]); index = parents[group][index]; }
  return { ...position, legs: position.legs.map(leg => {
    const strike = chosen.get(leg.strike)!, contract = byKey.get(`${leg.kind}:${strike}`)!;
    return { ...leg, strike, contract: contract.ticker, premium: contract.close?.price ?? Math.round(optionValue(leg.kind, strike, model) * 100) / 100,
      source: contract.close ? optionCloseLabel(contract.close) : "Modeled entry" };
  }) };
}
export const normalCDF = (x: number): number => {
  const z = Math.abs(x), t = 1 / (1 + .2316419 * z);
  const tail = Math.exp(-z * z / 2) / Math.sqrt(2 * Math.PI) * t * (.319381530 + t * (-.356563782 + t * (1.781477937 + t * (-1.821255978 + t * 1.330274429))));
  return x >= 0 ? 1 - tail : tail;
};
export const intrinsic = (kind: OptionKind, spot: number, strike: number) => Math.max(0, kind === "call" ? spot - strike : strike - spot);
export function optionValue(kind: OptionKind, strike: number, model: ModelInputs): number {
  const { spot, days, volatility, rate, dividend } = model;
  if (days <= 0) return intrinsic(kind, spot, strike);
  const t = days / 365, r = rate / 100, q = dividend / 100, v = volatility / 100;
  const s = spot * Math.exp(-q * t), k = strike * Math.exp(-r * t);
  if (spot <= 0 || strike <= 0 || v <= 0) return intrinsic(kind, s, k);
  const d1 = (Math.log(spot / strike) + (r - q + v * v / 2) * t) / (v * Math.sqrt(t));
  const d2 = d1 - v * Math.sqrt(t);
  return Math.max(0, kind === "call" ? s * normalCDF(d1) - k * normalCDF(d2) : k * normalCDF(-d2) - s * normalCDF(-d1));
}
export const entryCost = (p: Position) => p.shares * p.stockEntry + p.legs.reduce((s, l) => s + 100 * l.side * l.quantity * l.premium, 0);
export const entryFees = (p: Position) => p.legs.reduce((s, l) => s + l.quantity * p.fee, 0);
export function profitAt(p: Position, model: ModelInputs): number {
  return p.shares * (model.spot - p.stockEntry) + p.legs.reduce((s, l) => s + l.side * l.quantity * 100 * (optionValue(l.kind, l.strike, model) - l.premium), 0) - entryFees(p);
}
export function expirationRisk(p: Position) {
  const points = [...new Set([0, ...p.legs.map(l => l.strike)])].sort((a, b) => a - b);
  const payoff = (spot: number) => profitAt(p, { spot, days: 0, volatility: 0, rate: 0, dividend: 0 });
  const values = points.map(payoff);
  const slope = p.shares + p.legs.filter(l => l.kind === "call").reduce((s, l) => s + l.side * l.quantity * 100, 0);
  const roots: number[] = [];
  for (let i = 0; i < points.length; i++) {
    if (Math.abs(values[i]) < 1e-8) roots.push(points[i]);
    if (i && values[i] * values[i - 1] < 0) roots.push(points[i - 1] - values[i - 1] * (points[i] - points[i - 1]) / (values[i] - values[i - 1]));
  }
  const last = points.length - 1;
  if (slope && -values[last] / slope > 0) roots.push(points[last] - values[last] / slope);
  return { maxProfit: slope > 0 ? Infinity : Math.max(0, ...values), maxLoss: slope < 0 ? Infinity : Math.max(0, ...values.map(v => -v)), breakEvens: [...new Set(roots)].sort((a, b) => a - b) };
}
/** Position sensitivities by central differences. Theta uses a one-calendar-day step. */
export function positionGreeks(p: Position, m: ModelInputs) {
  const h = Math.max(.0001, m.spot * .0001), value = profitAt(p, m);
  const up = profitAt(p, { ...m, spot: m.spot + h }), down = profitAt(p, { ...m, spot: Math.max(0, m.spot - h) });
  return { delta: (up - down) / (2 * h), gamma: (up - 2 * value + down) / (h * h), theta: profitAt(p, { ...m, days: Math.max(0, m.days - 1) }) - value, vega: (profitAt(p, { ...m, volatility: m.volatility + .01 }) - profitAt(p, { ...m, volatility: Math.max(0, m.volatility - .01) })) / (m.volatility < .01 ? .01 : .02) };
}
export const strategyTemplates = [
  { id: "long-call", name: "Long call", outlook: "bullish", description: "Pay a premium for upside exposure. Loss is limited to the premium.", legs: [["call", 1, 1]], shares: 0 },
  { id: "bull-call", name: "Bull call spread", outlook: "bullish", description: "Buy a call and sell a higher strike. Both gain and loss are capped.", legs: [["call", 1, 1], ["call", -1, 1.1]], shares: 0 },
  { id: "covered-call", name: "Covered call", outlook: "bullish", description: "Own 100 shares and sell a call. Upside is capped; stock downside remains.", legs: [["call", -1, 1.05]], shares: 100 },
  { id: "long-put", name: "Long put", outlook: "bearish", description: "Pay a premium for downside exposure. Loss is limited to the premium.", legs: [["put", 1, 1]], shares: 0 },
  { id: "bear-put", name: "Bear put spread", outlook: "bearish", description: "Buy a put and sell a lower strike to reduce the debit and cap gains.", legs: [["put", 1, 1], ["put", -1, .9]], shares: 0 },
  { id: "iron-condor", name: "Iron condor", outlook: "range", description: "Two credit spreads with defined risk. Benefits from finishing between short strikes.", legs: [["put", 1, .9], ["put", -1, .95], ["call", -1, 1.05], ["call", 1, 1.1]], shares: 0 },
  { id: "straddle", name: "Long straddle", outlook: "move", description: "Buy a call and put at one strike. A large move or higher volatility can help.", legs: [["call", 1, 1], ["put", 1, 1]], shares: 0 },
  { id: "strangle", name: "Long strangle", outlook: "move", description: "Buy out-of-the-money calls and puts. Requires a larger move at expiration.", legs: [["put", 1, .95], ["call", 1, 1.05]], shares: 0 },
] as const;
export function createStrategy(id: string, model: ModelInputs): Position {
  const template = strategyTemplates.find(t => t.id === id) ?? strategyTemplates[0];
  return { shares: template.shares, stockEntry: model.spot, fee: .65, legs: template.legs.map(([kind, side, ratio], i) => {
    const strike = Math.max(.01, Math.round(model.spot * ratio * 100) / 100);
    return { id: `leg-${i}`, kind, side, strike, premium: Math.round(optionValue(kind, strike, model) * 100) / 100, quantity: 1, source: "Modeled entry" };
  }) };
}
export function daysUntil(today: string, expiry: string) { return Math.max(0, Math.round((Date.parse(`${expiry}T00:00:00Z`) - Date.parse(`${today}T00:00:00Z`)) / 86400000)); }
