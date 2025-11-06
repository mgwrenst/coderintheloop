🧠 Benchmark Query Categories — Summary Notes

This document explains each benchmark query category used in the structured MongoDB benchmark suite.
Each category is grounded in relational algebra and mapped to MongoDB’s aggregation framework.
Together they form a comprehensive set of test cases to measure expressiveness and performance.

📚 Table of Contents

Projection

Selection

Join

Union

Intersection

Difference

Alias (Renaming)

Aggregation

Quantifier — Exists (∃)

Quantifier — Forall (∀)

Transitive Closure

🧮 Projection

Concept: Choose specific attributes (columns) from documents.
Goal: Limit output to relevant fields or reshape data.
Mongo equivalent: $project

Typical patterns:

Select only a few top-level fields (navn, orgnr).

Compute or alias new fields (e.g. extract first person’s name).

Flatten nested arrays ($unwind + $project).

Example queries:

Show company name and orgnr.

Company + first person name.

Flatten all persons with their roles.

🔍 Selection

Concept: Filter documents by conditions.
Goal: Retrieve only those that meet specific logical predicates.
Mongo equivalent: $match, $expr, $and, $or

Typical patterns:

Equality or range checks.

Compound conditions combining multiple filters.

Array element filtering ($elemMatch).

Example queries:

Politicians from Oslo.

Companies updated after a date.

Firms with >3 people in Oslo or Bergen.

🔗 Join

Concept: Combine data from multiple collections based on key relationships.
Goal: Enrich data or simulate SQL joins.
Mongo equivalent: $lookup, $unwind, $project

Typical patterns:

Inner joins via shared fields (e.g. orgnr).

Chained lookups for nested relations.

Reverse joins (lookup in the opposite direction).

Example queries:

Companies and their owners.

Owners’ names and years.

Include owner municipality via reverse join.

➕ Union

Concept: Combine result sets from multiple collections.
Goal: Merge datasets with similar structure.
Mongo equivalent: $unionWith

Typical patterns:

Merge persons and politicians into a single list.

Tag source dataset (type: politiker / selskapsperson).

Consolidate data from eierskap and aksjeeiebok.

Example queries:

Names of all people (politicians + company persons).

Unified dataset of ownership information.

Annotated union by source type.

⚖️ Intersection

Concept: Find overlapping documents between datasets.
Goal: Identify entities present in both sources.
Mongo equivalent: $lookup + $match + $expr:$in

Typical patterns:

Compare names or identifiers.

Detect shared entities across collections.

Example queries:

People who are both politicians and company persons.

Companies whose names appear in ownership data.

Shareholders also listed as employees.

➖ Difference

Concept: Return documents in one set that are not in another.
Goal: Detect gaps or missing entities.
Mongo equivalent: $lookup + $not + $in

Typical patterns:

“A but not B” logic for dataset comparison.

Identify missing records or anomalies.

Example queries:

Politicians not in companies.

Companies missing ownership entries.

Shareholders not found in ownership collection.

🏷️ Alias (Renaming)

Concept: Rename or restructure document fields.
Goal: Normalize schemas, improve readability, or prepare export.
Mongo equivalent: $project

Typical patterns:

Rename fields (navn → company_name).

Compose new names using $concat.

Flatten nested structures (personer.navn → person).

Example queries:

Rename company and municipality fields.

Create a display name field.

Flatten nested person data.

📊 Aggregation

Concept: Summarize or compute aggregate metrics.
Goal: Produce statistics or rankings.
Mongo equivalent: $group, $sum, $avg, $sort, $limit

Typical patterns:

Group by location, year, or owner.

Calculate sums, averages, or counts.

Combine with $project or $sort for presentation.

Example queries:

Count companies per municipality.

Average ownership per year.

Top 5 municipalities by company size.

∃ Quantifier — Exists

Concept: Check if some element meets a condition.
Goal: Implement existential logic (there exists).
Mongo equivalent: $match with $elemMatch or pattern test.

Typical patterns:

Find arrays containing an element that matches criteria.

Search for presence using regex or equality.

Example queries:

Companies with at least one “Daglig leder.”

Owners based in Norway.

Persons with roles containing “styre.”

∀ Quantifier — Forall

Concept: Check if all elements meet a condition.
Goal: Implement universal quantification (for all).
Mongo equivalent: $setEquals, $allElementsTrue, $group + checks.

Typical patterns:

Test every array element for a property.

Aggregate groups where all members satisfy a condition.

Example queries:

All persons are “Styremedlem.”

All persons have a municipality.

All party members are elected (innvalgt = true).

♻️ Transitive Closure

Concept: Recursively expand relationships across multiple levels.
Goal: Compute hierarchical or indirect relationships.
Mongo equivalent: $graphLookup

Typical patterns:

Recursive ownership or dependency resolution.

Limit recursion depth (maxDepth).

Compute summary of unique reachable nodes.

Example queries:

Find indirect owners through multi-level ownership.

Trace ownership up to depth 3.

Count unique owners in a hierarchy.

✅ Notes

These categories together cover:

All core relational operators (projection, selection, join, union, difference, intersection).

Derived operators like aliasing and aggregation.

Logical quantifiers (exists / forall) for expressive power.

Recursive closure for hierarchical data.

This suite provides both functional coverage (expressiveness) and performance diversity (complexity from 1 → 5).