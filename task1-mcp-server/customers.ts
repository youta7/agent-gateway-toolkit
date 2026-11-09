// In-memory customer store. All money is held as integer cents so that
// arithmetic is exact; floating-point dollars only appear at the API boundary.
export type CustomerStatus = "active" | "suspended";

export interface Customer {
  customerId: string;
  name: string;
  email: string;
  status: CustomerStatus;
  balanceCents: number;
}

const seed: Customer[] = [
  { customerId: "CUST-00001", name: "Ada Lovelace", email: "ada@example.com", status: "active", balanceCents: 125_050 },
  { customerId: "CUST-00002", name: "Grace Hopper", email: "grace@example.com", status: "active", balanceCents: 4_999 },
  { customerId: "CUST-00003", name: "Alan Turing", email: "alan@example.com", status: "suspended", balanceCents: 80_000 },
];

export const customers = new Map<string, Customer>(seed.map((c) => [c.customerId, c]));

/** Converts a dollar amount to integer cents, rounding half away from zero. */
export function toCents(amount: number): number {
  return Math.round(amount * 100);
}

/** Formats integer cents as a dollar string with two decimals. */
export function fromCents(cents: number): string {
  const sign = cents < 0 ? "-" : "";
  const abs = Math.abs(cents);
  return `${sign}${Math.floor(abs / 100)}.${String(abs % 100).padStart(2, "0")}`;
}
