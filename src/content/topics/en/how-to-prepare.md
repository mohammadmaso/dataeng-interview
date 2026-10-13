---
order: 5
tid: "how-to-prepare"
track: "0"
section: null
num: "0.6"
title: "How to Prepare in 8 Weeks"
type: "guide"
level: "beginner"
priority: "P0"
est_hours: 1
summary: "Study method: quick pass over the whole map, then deep work on P0s, daily SQL practice, at least two timed system design problems per week, and mock interviews. Full plan in Appendix A."
covers: ["spaced repetition","active recall","mock interviews","tracking weak spots"]
interview_qs: []
extras: []
prerequisites: []
related: ["interview-loop", "delivery-framework", "sql-window-functions", "behavioral-questions", "senior-expectations", "project-narratives"]
---

## 🎯 TL;DR

> Eight weeks is enough if you **practise retrieval** (answering from memory) more than you **re-read**. Plan in phases: map, fundamentals, timed practice, mock interviews, repair weak spots, taper.

- Spend about 60–90 minutes a day, split between SQL, one concept block, and recall.
- Do at least two timed system design problems a week. Reading designs is not the same as producing one under time pressure.
- Run mock interviews from week 6. Feedback from another person finds the gaps you cannot see.
- Track weak spots in one table and review them on a schedule (days 1, 3, 7, 14).

## 🌱 Start simple (Junior)

> **💡 Analogy:** Training for a 10 km run. Reading about pacing does not make you faster. You run, you time yourself, you look at the times, and you run the slow parts again. Interview prep works the same way.

A simple daily routine:

- **30 minutes of SQL.** One problem from a practice set, written by hand or in an editor, with no hints.
- **30 minutes of one concept.** Read a lesson, then close it and write three sentences from memory.
- **Once or twice a week.** One design problem with a 45-minute timer.

Do not try to learn everything. Start with the P0 topics on the roadmap and leave the rest for later.

> **🎤 Say this in the interview:** "I prepare by practising under time limits and checking my own recall, not by reading more notes."

## 🔧 How it really works (Mid)

The study method rests on three learning effects:

- **Active recall.** Trying to answer without looking is the strongest way to remember. Reading again feels productive but builds weaker memory.
- **Spaced repetition.** Reviewing a topic after 1, 3, 7 and 14 days keeps it in memory with less total time than cramming.
- **Feedback.** You learn the gap only when someone (or a clear answer key) tells you it is there.

**An 8-week plan:**

| Week | Focus | Output at the end |
|---|---|---|
| 1 | Map the whole roadmap; diagnostic: one SQL, one Python, one design problem | Baseline score per signal |
| 2–3 | P0 fundamentals: SQL joins and windows, Python data handling, batch vs streaming, storage formats | A one-page summary written from memory per topic |
| 4–5 | P0 deep dives and design problems, two timed designs per week | Designs with numbers and deep dives |
| 6 | Start mock interviews (two per week) with peers or a timer | Feedback list, top 3 gaps |
| 7 | Repair weak spots from the tracker; redo the weakest design | Weak spots below 3 out of 5 |
| 8 | Taper: project stories, behavioral answers, final mocks, sleep | Ready stories for each project |

The baseline in week 1 matters. Without it you cannot tell whether the next four weeks helped.

**Worked example: a tracker.** One row per topic, updated after each session:

| Topic | Confidence (1–5) | Last review | Next review | Typical error |
|---|---|---|---|---|
| Window functions | 2 | 2026-03-02 | 2026-03-04 | Forgets default frame |
| Kafka delivery semantics | 3 | 2026-03-02 | 2026-03-05 | Mixes up at-least-once and exactly-once |
| Late data and watermarks | 2 | 2026-03-03 | 2026-03-04 | Cannot say what a watermark drops |

Sort by confidence every Sunday. The bottom three rows are next week's focus. A topic moves to a longer review interval only after you get it right from memory twice in a row.

## 🧠 Senior depth & trade-offs

- **Prioritise by return.** P0 topics first, then the topics that appear in your target companies' stacks. Do not chase all 200 items. Depth on the core beats shallow coverage of the periphery.
- **Mock interviews need a rubric.** Score yourself per signal (requirements, numbers, trade-offs, operations), not on "I think it went well". Record the session; you will notice filler words and missed numbers.
- **Practise trade-off language.** Senior loops reward "X is better when Y, but it costs Z". Write three such sentences per topic and say them out loud.
- **Mix languages on purpose.** Many interviews in Iran run in Persian with English technical terms. Practise explaining a Kafka or Spark concept in that mixed style, so the terms do not freeze you.
- **Know your own projects better than the roadmap.** The project deep dive is where the score is decided. Prepare two projects with numbers, a failure, and a decision you would reverse.
- **Measure the plan, not the hours.** Hours studied are a poor proxy. Count timed problems solved, mocks done and weak spots closed.

## 🧪 Try it

1. Create the tracker from the worked example with at least six topics from the P0 list. Fill in your confidence honestly.
2. Schedule the first three reviews for your top weak topic (days 1, 3 and 7).
3. Write one 45-second project story with a number, a failure and a decision.

<details><summary>Show solutions</summary>

**1–2.** There is no single answer. The check: every weak row has a next-review date, and no row has a confidence score without an error note.

**3.** A good story: "We ingested 40 million events a day into a warehouse. The first version missed late events and undercounted by about 3% at the day boundary. I added a watermark and a daily reconciliation against the source, and the gap fell below 0.1%." Replace the numbers with your own, and make sure every number is one you can defend.

</details>

## 💬 Interview corner

> **🎤 Say this in the interview:** "I prepare by timing myself on design problems, running mock interviews from mid-way through, and tracking the weak topics until I can explain them from memory."

**How it is asked:** "How do you prepare for technical interviews?" or "How do you keep learning?"

- **Junior answer:** "I watch videos and read documentation."
- **Mid answer:** names a schedule, mentions timed practice and mock interviews, and gives one example of a gap they closed.
- **Senior answer:** describes how they measure progress, how they choose what to skip, and how they use feedback from peers and production incidents to shape their learning.

**Model answer (3–5 sentences):** "I start with a baseline on SQL, Python and one design problem, then focus on the core topics with the most return. I practise by recall and with a timer, and I do two design problems a week. From the midpoint I run mock interviews and track each weak topic with a review schedule. In the last week I focus on my project stories and trade-off answers rather than new material."

## ⚠️ Common mistakes

- **Re-reading instead of recalling.** It feels productive and produces little memory.
- **Studying tools without solving problems.** Knowing what Kafka is does not prepare you for a delivery-semantics question.
- **No timed practice.** Untimed design problems hide the real problem: running out of time.
- **Skipping mocks until the last week.** Feedback needs time to change what you do.
- **Memorising model answers.** Interviewers ask one follow-up and the memorised answer breaks.
- **Ignoring weak spots after the baseline.** The tracker only helps if you act on it every week.

## ✅ Check yourself (quiz)

1. Which study activity is most effective for long-term recall?
   - A) Re-reading your notes three times
   - B) Highlighting key sentences in a lesson
   - C) Closing the notes and writing the answer from memory, then checking
   - D) Watching a long video at 2× speed

<details><summary>Answer</summary>

**C** — active recall forces retrieval, which is the strongest memory technique. Re-reading and highlighting feel productive but leave a weak memory trace.

</details>

2. In which week should your mock interviews start, and why?
   - A) Week 1, before you know anything
   - B) Around week 6, after the fundamentals, so the feedback targets real gaps
   - C) Week 8 only, to test everything at once
   - D) Never; mocks are not needed for data engineering

<details><summary>Answer</summary>

**B** — mocks are most useful once you have fundamentals to test. Starting too early gives feedback on things you have not learned yet, and starting in week 8 leaves no time to repair the gaps they find.

</details>

3. You are reviewing a tracker row: "Late data and watermarks, confidence 2, last review 3 days ago." What should the next step be?
   - A) Mark it done and move on
   - B) Review it again on the scheduled day, with a recall attempt, and give it another interval only after two correct recalls in a row
   - C) Read the whole book on streaming before the next session
   - D) Remove the row so the tracker stays short

<details><summary>Answer</summary>

**B** — the schedule and the recall test decide when a topic is safe. Low confidence means the next review comes soon, and the interval grows only after the topic is recalled correctly.

</details>

4. **(Senior)** You have three weeks left and your mock feedback says "trade-offs are vague" in design rounds. What is the most effective change?
   - A) Learn ten new tools to sound broader
   - B) Write three trade-off sentences for each core topic, say them out loud in mock rounds, and time each answer
   - C) Memorise one perfect design and repeat it
   - D) Stop doing design problems and focus only on SQL

<details><summary>Answer</summary>

**B** — trade-off language is a skill that improves through deliberate repetition with feedback. Breadth without trade-offs does not fix the gap, and memorised designs break on the first follow-up.

</details>

## 🗂️ Flashcards

| Front | Back |
|---|---|
| What is active recall? | Answering from memory before checking the notes |
| Why use spaced repetition? | Reviews after 1, 3, 7, 14 days keep memory with less total study time |
| When do mock interviews start? | Around week 6, once the fundamentals exist to test |
| What goes in the baseline (week 1)? | One SQL, one Python and one design problem, scored per signal |
| What is the review rule for a weak topic? | Review soon; grow the interval only after two correct recalls in a row |
| Why are hours a poor progress measure? | They do not show skill; count timed problems, mocks and closed weak spots |
| What should the final week focus on? | Project stories, trade-off answers and final mocks, not new material |

## 🔗 Go further

- **interview-loop** explains which rounds your preparation must cover.
- **delivery-framework** is the structure to practise in the timed design sessions.
- **sql-window-functions** is a good first deep dive for the SQL block.
- **behavioral-questions** and **project-narratives** prepare the stories for the last week.
- **senior-expectations** describes what the mock feedback is measured against at each level.
