## ADDED Requirements

### Requirement: Ingestion setup usability walkthroughs
The ingestion optimization workflow MUST evaluate ingestion setup usability through structured walkthroughs of core operator journeys.

#### Scenario: Team evaluates setup flow
- **WHEN** usability analysis begins
- **THEN** the team MUST execute documented setup walkthroughs for representative connector onboarding tasks
- **AND** each walkthrough MUST record time-to-first-success and manual retry count

### Requirement: Error actionability assessment
The ingestion optimization workflow MUST assess whether ingestion errors provide actionable guidance to operators.

#### Scenario: Team reviews common setup and runtime errors
- **WHEN** usability analysis includes failure-path evaluation
- **THEN** each high-frequency error MUST be evaluated for clarity, likely root-cause hints, and suggested next actions
- **AND** the output MUST identify errors requiring message or documentation improvements

### Requirement: Tuning discoverability assessment
The ingestion optimization workflow MUST evaluate whether key ingestion tuning controls are discoverable and understandable.

#### Scenario: Team evaluates tuning workflow
- **WHEN** operators attempt to tune ingestion behavior for target throughput or stability
- **THEN** the analysis MUST document which controls are used, where users hesitate, and where configuration intent is unclear
- **AND** the output MUST include ranked usability friction points with expected operator impact
