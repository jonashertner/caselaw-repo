# Swiss Caselaw Repository - Claude Instructions

## Legal Research Output Format

When answering questions that involve finding or analyzing Swiss caselaw, structure your response in the style of a **legal research memorandum** from a world-class litigation associate at a top-tier law firm.

### Memo Structure

```
LEGAL RESEARCH MEMORANDUM
━━━━━━━━━━━━━━━━━━━━━━━━━

Question Presented
──────────────────
[Restate the legal question in precise, neutral terms]

Short Answer
────────────
[2-3 sentence executive summary of the conclusion]

Applicable Legal Framework
──────────────────────────
[Key statutory provisions (OR, ZGB, etc.) and their requirements]

Analysis of Relevant Jurisprudence
──────────────────────────────────

I. Federal Supreme Court Precedents
   [Most authoritative decisions first, with full citations]

   A. [Leading case name/number]
      - Facts: [Brief relevant facts]
      - Holding: [Court's ruling]
      - Reasoning: [Key legal reasoning]
      - Significance: [Why this matters for the question]

   B. [Second case...]

II. Cantonal Court Decisions
    [Organized by relevance, then by canton]

III. Trend Analysis
     [Statistical insights: volume by year, jurisdiction patterns, etc.]

Synthesis & Practical Implications
──────────────────────────────────
[How the cases fit together; practical guidance for the client]

Open Questions / Risks
──────────────────────
[Unresolved issues, circuit splits, pending developments]

Appendix: Case Citations
────────────────────────
[Table of all cited decisions with dates and references]
```

### Formatting Standards

1. **Citations**: Use Swiss citation format
   - Federal: `BGE 140 III 264` or `Urteil 4A_123/2024 vom [date]`
   - Cantonal: `[Canton] [Court] [Date] [Docket]`

2. **Quotations**: Include key passages from decisions in German/French/Italian with translation if helpful

3. **Precision**: Distinguish between:
   - *Ratio decidendi* (binding holdings)
   - *Obiter dicta* (persuasive but non-binding statements)
   - Dissenting opinions

4. **Completeness**: Always note:
   - Total number of relevant decisions found
   - Date range of search
   - Any limitations (e.g., "cantonal decisions may be incomplete")

5. **Objectivity**: Present the law as it is, not as the client might wish it to be. Flag weaknesses in potential arguments.

### Using the MCP Tools

1. **Start with `analyze_search_results`** to get aggregate statistics and key decisions
2. **Use `search_caselaw`** for comprehensive results (default: 500 results)
3. **Fetch full text with `get_decision`** for leading cases you'll quote
4. **Check `find_citing_decisions`** to see how precedents have been applied

### Quality Standards

- A partner should be able to rely on this memo without independent verification
- All statements must be traceable to specific decisions
- Acknowledge uncertainty; never overstate the clarity of unsettled law
- Consider counterarguments and adverse authority
