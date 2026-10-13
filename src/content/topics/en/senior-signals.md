---
order: 189
tid: "senior-signals"
track: "16"
section: null
num: "16.3"
title: "Senior Signals"
type: "guide"
level: "senior"
priority: "P0"
est_hours: 2
summary: "Signals interviewers look for: ownership, handling ambiguity, impact beyond the team, mentoring, data-driven decisions, managing tech debt, and stakeholder negotiation."
covers: []
interview_qs: []
extras: []
prerequisites: ["star-framework"]
related: ["senior-expectations", "negotiation-iran", "interview-loop", "project-narratives"]
---

## 🎯 TL;DR

> A **senior signal** is evidence that you act beyond your own tasks: you own outcomes, handle unclear problems, multiply other people, and make decisions that the business can measure.

- Interviewers do not check a list of years. They look for **behaviours** that show up in your stories.
- The seven signals are: **ownership**, **ambiguity**, **impact beyond the team**, **mentoring**, **data-driven decisions**, **managing tech debt**, and **stakeholder negotiation**.
- Each signal needs **one concrete story**, with your action, a number, and a trade-off.
- A senior answer is not longer. It is **wider**: it mentions the people, the cost, and the follow-up work.

## 🌱 Start simple (Junior)

> **💡 Analogy:** A junior cook follows the recipe. A senior cook notices the oven runs hot, changes the order of the dishes so the kitchen is not blocked, and teaches the new cook how to do the same. Both cook well. The senior one makes the whole kitchen better.

The difference is not seniority in years. It is the **size of the problem** you can own. A junior owns a task. A senior owns an outcome: "the report is correct and on time, and I know when it is not".

Here is a simple self-test. For any project, answer these three questions:

1. Did I **notice** a problem that nobody asked me to fix?
2. Did I **decide** something that others then followed?
3. Did I **leave** the team better at doing this without me?

If you can answer "yes" to two of the three, you have a senior story. If none, pick a different project or look harder at your role.

> **⚠️ Watch out:** Do not claim a signal you cannot show. "I mentored people" needs one named person and one thing they did afterwards. Interviewers will ask.

## 🔧 How it really works (Mid)

Each signal is a pattern of behaviour. The table shows how a mid-level and a senior-level answer differ for the same signal.

| Signal | Mid-level answer sounds like | Senior-level answer sounds like |
|---|---|---|
| Ownership | "I fixed the bug I was assigned." | "I noticed the alert was noisy, so I owned the whole on-call runbook and cut pages by half." |
| Ambiguity | "I asked for clearer requirements." | "I wrote three options with costs, got the owner to choose, and recorded the decision." |
| Impact beyond team | "My pipeline helped my team." | "The shared customer table I built is now used by four teams and two finance reports." |
| Mentoring | "I helped a new hire with questions." | "I set up a pairing plan for a new hire; they now own a domain alone after six weeks." |
| Data-driven decisions | "I checked the numbers before deploying." | "I ran a two-week A/B comparison and changed the design based on cost per run." |
| Tech debt | "I refactored some code when I had time." | "I put the debt on the roadmap with a cost estimate and got a fixed slot each sprint." |
| Negotiation | "I agreed with the product team." | "I offered two scope options with different delivery dates and let the owner pick the trade-off." |

The pattern is the same in every row: **a problem, a choice, a mechanism, and a measurable effect**.

**Ownership** means you follow the outcome past the edge of your ticket. You check the output, you watch the alert after the change, and you tell the owner when something is wrong.

**Ambiguity** is normal in data work: the requirement says "make the dashboard faster". A senior turns that into a question with options, costs and a recommendation, then moves.

**Impact beyond the team** is about reuse and leverage: a schema, a contract, a template, or a platform capability that other teams adopt without you in the room.

**Mentoring** is measured by what the other person can do afterwards, not by the hours you spent.

**Data-driven decisions** means you name the metric before you change the system, and you accept a result that contradicts your preference.

**Managing tech debt** is making the cost visible. Debt that is never named is never paid. Show how you converted a vague complaint into an estimate and a schedule.

**Stakeholder negotiation** means presenting trade-offs in the other person's currency. A product owner cares about dates and risk, a finance lead cares about cost, an engineer cares about operations. Speak to each in their terms.

**Worked example (illustrative, replace with your own).** A nightly job that builds a customer summary cost too much on the cloud bill and missed its window twice a month. A senior-signal story covers several signals at once.

> **Weak answer:** "We had a costly job, so I optimised it and it got cheaper. The team liked it."

> **Strong answer:** "**S:** Our nightly customer summary cost about 40% of the data platform budget and missed its 7 AM window twice a month, which delayed the sales report. **T:** I owned the job, but its inputs came from three teams. My goal was a 50% cost cut without changing the numbers. **A:** I first asked the sales team which columns they really used; 18 of 60 were unused. I then benchmarked two options on a week of data: partitioning by date with pruning, or a smaller incremental build. The incremental build cut cost by 62% and runtime by 70%, but it needed a reconciliation check, which I wrote and ran daily for a month. I wrote a one-page design doc, got the three teams to agree the column list, and documented the rollback. **R:** Monthly cost fell from about 40% to about 15% of the budget share, the job has finished before 7 AM for 90 days, and the other two teams now use the same reconciliation template. **Reflection:** I should have made the column list a contract from the start; it would have saved two weeks of back-and-forth."

Notice how the strong answer shows ownership (watching the outcome), data-driven decisions (the benchmark), negotiation (the column list agreed by three teams), impact beyond the team (the template), and tech debt (the unused columns). One story, five signals.

## 🧠 Senior depth & trade-offs (Senior)

At senior level, the signals connect to **system-level** decisions. Interviewers probe for these depth markers:

- **Operational maturity.** A senior's pipeline has an owner, a runbook, an SLO, an alert that someone acts on, and a documented rollback. Mention these without being asked.
- **Cost awareness.** Know the unit economics: cost per run, cost per million rows, cost per dashboard. A senior can say "the cost is mostly shuffle, so the fix is partitioning, not a bigger cluster".
- **Contracts and boundaries.** Seniors reduce coupling. A data contract with a producer, a versioned schema, or a clear consumer list is a stronger signal than a faster query.
- **Decision records.** A short written decision with its alternatives, date, and owner shows that the decision can survive the people who made it.
- **Failure of the signal itself.** Be careful with "hero" stories, where one person saves the day every week. Interviewers read them as a team risk. Show the system you built so the next person does not need the hero.

**Depth versus breadth.** A senior is expected to go deep in one area and be competent in several. If all your stories are about one tool, show a second area. If all your stories are cross-team coordination, show one technical deep-dive as well.

**Signals are not seniority by title.** A mid-level engineer who writes the decision record and runs the negotiation shows senior signals. The interviewer is checking behaviour, and a title does not replace evidence.

## 🧪 Try it

**Exercise 1: Audit your stories.** Make a table with seven rows (the seven signals) and two columns: "story I can tell" and "evidence (number or named person)". Fill in what you have. Mark any row with no story.

**Exercise 2: Upgrade one mid answer.** Take this answer and rewrite it as a senior answer that shows at least two signals:

> "I helped the team by writing documentation for our pipeline."

**Exercise 3: Find the number.** For each claim below, name the number or the named person the interviewer would ask about:

- "I improved the data quality of the sales table."
- "I helped a junior engineer grow."
- "I reduced our costs."

**Exercise 4: Trade-off phrasing.** Practise saying one negotiation in two sentences: the two options you offered, and the trade-off that the other side chose.

## 💬 Interview corner

> **🎤 Say this in the interview:** "Tell me about a time you influenced a decision outside your team." Name the signal in your head (negotiation and impact), then tell the story in STAR(R) order.

**Mid-level model answer (short):** "Our analytics team needed a customer table that matched the billing system. I built a first version and shared it with two teams, and I fixed the mismatches they reported. The table became the standard for their dashboards."

**Senior-level model answer (short):** "The analytics and billing teams defined 'active customer' differently, and their reports disagreed by 8%. I ran both definitions on three months of data, showed the difference by segment, and proposed one definition with a documented exception list. Both leads agreed after one review meeting, and I wrote the contract and the tests. Six months later, four teams consume the table, and the monthly disagreement is under 0.5%."

## ⚠️ Common mistakes

- **Listing signals without stories.** "I am a good mentor" is a claim, not a signal.
- **Measuring effort instead of effect.** "I worked many nights" is not impact.
- **Taking credit for the team's output.** Name the decisions you made and the others' contributions.
- **Stopping at delivery.** Seniors watch the outcome after the release. Say what you checked later.
- **Only technical stories.** Senior roles include people and process. Show at least one negotiation or mentoring example.
- **Ignoring cost and risk.** A design without cost or failure modes sounds like a demo.

## ✅ Check yourself (quiz)

1. Which answer best shows the "impact beyond the team" signal?
   - A) "My pipeline was used by my team every day."
   - B) "I wrote a shared customer table that four teams now use, and I published the contract."
   - C) "I worked hard on the project."
   - D) "I was promoted after the project."

<details><summary>Answer</summary>

**B** — it shows reuse by others and a durable artifact (the contract). Effort and promotion do not show leverage.

</details>

2. A product owner wants a feature in two weeks. The data model cannot support it without a refactor. What is the senior response?

   - A) Refuse and wait for a better plan
   - B) Build it quickly without telling anyone
   - C) Offer two scope options with their dates and risks, and let the owner choose the trade-off
   - D) Ask a manager to decide

<details><summary>Answer</summary>

**C** — this is stakeholder negotiation with a clear trade-off. It keeps the decision with the owner and makes the cost of each option visible.

</details>

3. Which claim is the weakest evidence of mentoring?

   - A) "I trained a new hire, and they now own the billing domain alone after six weeks."
   - B) "I ran a weekly review for three juniors, and two of them shipped a pipeline to production."
   - C) "I helped people when they asked."
   - D) "I wrote the onboarding guide, which the team now updates."

<details><summary>Answer</summary>

**C** — it has no named outcome and no repeated mechanism. The others name what the person can do afterwards.

</details>

4. **(Senior)** An interviewer says your story is a "hero" story. What is the most likely concern, and how do you answer?

   - A) You worked too many hours
   - B) The success depended on one person, which is a team risk; explain the system or process you built so others can repeat it
   - C) You used too many tools
   - D) The project was too small

<details><summary>Answer</summary>

**B** — hero stories suggest fragile teams. Show the runbook, the contract, the review, or the automation that made the result repeatable.

</details>

## ✅ Self-assessment checklist

- [ ] I have at least one story for each of the seven signals
- [ ] Each story has an action I took, a number or named person, and a trade-off
- [ ] I can say how the outcome was checked after delivery
- [ ] At least one story shows an effect outside my own team
- [ ] I can describe a negotiation in terms of the other side's currency
- [ ] I can explain how a piece of tech debt was made visible and scheduled
- [ ] None of my stories depends on heroics that cannot be repeated

## 🗂️ Flashcards

| Front | Back |
|---|---|
| What is a senior signal? | Evidence that you act beyond your task: own outcomes, handle ambiguity, multiply others |
| Ownership, in one sentence | You follow the outcome past your ticket and check it after release |
| How do you show mentoring? | A named person who can do something new afterwards |
| What makes a decision data-driven? | A metric named before the change, and acceptance of contrary results |
| How do you manage tech debt in an interview? | Show how you made the cost visible, estimated it, and scheduled it |
| What does good negotiation look like? | Two or more options with costs and dates; the owner chooses the trade-off |
| Why is a hero story a risk? | It suggests the result cannot be repeated without one person |

## 🔗 Go further

- [[senior-expectations]] explains the expected depth at each level, which these signals should match.
- [[star-framework]] gives the structure for turning each signal into a story.
- [[project-narratives]] shows how the same signals appear inside your deep-dive projects.
- [[interview-loop]] explains which rounds are most likely to test these signals.
- [[negotiation-iran]] covers salary and offer-package negotiation on the Iranian market, including how to handle multiple offers.
