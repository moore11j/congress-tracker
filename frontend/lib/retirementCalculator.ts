export type RetirementPerson = { age: number; retirementAge: number; openingBalance: number; monthlySavings: number; monthlyIncome: number };
export type RetirementPlan = { you: RetirementPerson; spouse: RetirementPerson; includeSpouse: boolean; annualReturn: number; retirementReturn: number; inflation: number; taxRate: number; retirementYears: number };
export type ProjectionRow = { year: number; youAge: number; spouseAge: number; you: number; spouse: number; total: number; contributions: number; growth: number; withdrawals: number; income: number; shortfall: number; phase: string };
export const defaultRetirementPlan: RetirementPlan = {
  you: { age: 35, retirementAge: 65, openingBalance: 50000, monthlySavings: 1000, monthlyIncome: 2500 },
  spouse: { age: 33, retirementAge: 65, openingBalance: 30000, monthlySavings: 750, monthlyIncome: 2000 },
  includeSpouse: true, annualReturn: 7, retirementReturn: 4, inflation: 2.5, taxRate: 15, retirementYears: 30,
};

export function realAnnualReturn(nominalReturnPct: number, inflationPct: number): number {
  return ((1 + nominalReturnPct / 100) / (1 + inflationPct / 100) - 1) * 100;
}

export function retirementDisplayValue(value: number, elapsedYears: number, inflationPct: number, real: boolean): number {
  return real ? value / Math.pow(1 + inflationPct / 100, elapsedYears) : value;
}

export function validateRetirementPlan(plan: RetirementPlan): string[] {
  const errors: string[] = [];
  const inRange = (n: number, min: number, max: number) => Number.isFinite(n) && n >= min && n <= max;
  for (const [name, person] of [["You", plan.you], ...(plan.includeSpouse ? [["Spouse", plan.spouse]] : [])] as [string, RetirementPerson][]) {
    if (!Number.isInteger(person.age) || !inRange(person.age, 18, 100)) errors.push(`${name}: current age must be a whole number from 18 to 100.`);
    if (!Number.isInteger(person.retirementAge) || !inRange(person.retirementAge, person.age, 100)) errors.push(`${name}: retirement age must be a whole number between current age and 100.`);
    if (!inRange(person.openingBalance, 0, 1e9)) errors.push(`${name}: opening balance must be between $0 and $1 billion.`);
    if (!inRange(person.monthlySavings, 0, 1e6) || !inRange(person.monthlyIncome, 0, 1e6)) errors.push(`${name}: monthly savings and income must be between $0 and $1 million.`);
  }
  if (!inRange(plan.annualReturn, -99, 100) || !inRange(plan.retirementReturn, -99, 100)) errors.push("Annual returns must be between -99% and 100%.");
  if (!inRange(plan.inflation, 0, 20)) errors.push("Inflation must be between 0% and 20%.");
  if (!inRange(plan.taxRate, 0, 60)) errors.push("Withdrawal tax rate must be between 0% and 60%.");
  if (!Number.isInteger(plan.retirementYears) || !inRange(plan.retirementYears, 1, 60)) errors.push("Years after both retire must be a whole number from 1 to 60.");
  return errors;
}

export function projectRetirement(plan: RetirementPlan, startYear: number) {
  const errors = validateRetirementPlan(plan);
  if (errors.length) throw new Error(errors.join(" "));
  const people = plan.includeSpouse ? [plan.you, plan.spouse] : [plan.you];
  const retirementMonths = people.map((p) => (p.retirementAge - p.age) * 12);
  const bothYear = Math.max(...retirementMonths) / 12;
  const firstYear = Math.min(...retirementMonths) / 12;
  const years = bothYear + plan.retirementYears;
  const balances = people.map((p) => p.openingBalance);
  const atRetirement = people.map((p, i) => retirementMonths[i] === 0 ? p.openingBalance : 0);
  const depletedMonth: (number | null)[] = people.map(() => null);
  const savingRate = Math.pow(1 + plan.annualReturn / 100, 1 / 12) - 1;
  const retirementRate = Math.pow(1 + plan.retirementReturn / 100, 1 / 12) - 1;
  const phase = (elapsed: number) => elapsed < firstYear ? "Saving" : elapsed < bothYear ? "One retired" : "Retirement";
  const rows: ProjectionRow[] = [{ year: startYear, youAge: plan.you.age, spouseAge: plan.spouse.age, you: balances[0], spouse: balances[1] ?? 0, total: balances.reduce((a, b) => a + b, 0), contributions: 0, growth: 0, withdrawals: 0, income: 0, shortfall: 0, phase: phase(0) }];
  let contributions = 0, growth = 0, withdrawals = 0, income = 0, shortfall = 0;
  for (let month = 1; month <= years * 12; month++) {
    people.forEach((person, i) => {
      const retired = month > retirementMonths[i];
      const gain = balances[i] * (retired ? retirementRate : savingRate);
      balances[i] += gain;
      growth += gain;
      if (!retired) { balances[i] += person.monthlySavings; contributions += person.monthlySavings; }
      else {
        // Income is entered in today's dollars, paid at month end, and grossed up for flat withdrawal tax.
        const desiredIncome = person.monthlyIncome * Math.pow(1 + plan.inflation / 100, month / 12);
        const desiredWithdrawal = desiredIncome / (1 - plan.taxRate / 100);
        const actualWithdrawal = Math.min(balances[i], desiredWithdrawal);
        const actualIncome = actualWithdrawal * (1 - plan.taxRate / 100);
        balances[i] = Math.max(0, balances[i] - actualWithdrawal);
        withdrawals += actualWithdrawal;
        income += actualIncome;
        shortfall += Math.max(0, desiredIncome - actualIncome);
        if (actualWithdrawal + 0.000001 < desiredWithdrawal && depletedMonth[i] === null) depletedMonth[i] = month;
      }
      if (month === retirementMonths[i]) atRetirement[i] = balances[i];
    });
    if (month % 12 === 0) {
      rows.push({ year: startYear + month / 12, youAge: plan.you.age + month / 12, spouseAge: plan.spouse.age + month / 12, you: balances[0], spouse: balances[1] ?? 0, total: balances.reduce((a, b) => a + b, 0), contributions, growth, withdrawals, income, shortfall, phase: phase(month / 12) });
      contributions = growth = withdrawals = income = shortfall = 0;
    }
  }
  return { rows, atRetirement, bothYear, firstYear, depletedMonth, bothRetired: rows[bothYear] };
}

/** Never substitute a multi-year total return or trade average for an annual return. */
export function annualizeReturn(totalReturn: number | null | undefined, start?: string | null, end?: string | null): number | null {
  if (typeof totalReturn !== "number" || !Number.isFinite(totalReturn) || totalReturn < -100 || !start || !end) return null;
  const days = (Date.parse(end) - Date.parse(start)) / 86400000;
  if (!Number.isFinite(days) || days < 365) return null;
  return (Math.pow(1 + totalReturn / 100, 365.25 / days) - 1) * 100;
}
