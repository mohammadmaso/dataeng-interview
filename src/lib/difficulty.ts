// Build-time difficulty estimate for question-bank items: 0 = junior, 1 = mid, 2 = senior.
// The score is a heuristic (answer depth, design/scale vocabulary, SQL complexity). Levels are assigned by
// rank *within a chapter* so every chapter gets a usable spread, and the UI labels them "estimated".
const SENIOR = /\b(design|architect\w*|scal\w+|trade-?offs?|partition\w*|consisten\w+|distributed|optimi[sz]\w+|bottleneck|migrat\w+|exactly-once|cap theorem|shard\w*|latency|throughput|fault[- ]toleran\w+|high availability|replicat\w+|idempoten\w+|backpressure|skew\w*|concurren\w+|isolation level|index\w*|execution plan|transaction\w*)\b/gi;
const SQL_COMPLEX = /\b(over\s*\(|partition by|with recursive|union|intersect|except|having|exists|correlated|cross apply|lateral|rank\(|row_number|lag\(|lead\(|pivot|case when)\b/gi;
const BASIC = /^\s*(what is|what are|what does|define|what do you mean|list|name|which|difference between|explain what)\b/i;
const strip = (h: string) => h.replace(/<[^>]+>/g, ' ').replace(/&[a-z]+;/g, ' ');

export function difficultyScore(q: string, a: string): number {
  const text = strip(a);
  let score = Math.log2(Math.max(40, text.length)) * 0.8;
  score += new Set((q + ' ' + text).match(SENIOR)?.map((x) => x.toLowerCase())).size * 0.6;
  score += (text.match(SQL_COMPLEX)?.length ?? 0) * 0.5;
  if (/\bhow would you|\bdesign\b|\bwhat happens (if|when)\b|\bscenario\b|\bhow do you (handle|ensure|scale)\b|\boptimi[sz]e\b/i.test(q)) score += 1.5;
  if (BASIC.test(q)) score -= 1;
  return score;
}

/** Bottom 35% of a chapter → junior, top 20% → senior, rest mid. */
export function assignLevels(scores: number[]): (0 | 1 | 2)[] {
  const order = scores.map((s, i) => [s, i] as const).sort((x, y) => x[0] - y[0] || x[1] - y[1]);
  const out: (0 | 1 | 2)[] = new Array(scores.length).fill(1);
  order.forEach(([, i], rank) => {
    const p = rank / Math.max(1, scores.length);
    out[i] = p < 0.35 ? 0 : p >= 0.8 ? 2 : 1;
  });
  return out;
}
