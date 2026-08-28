package semantics

// Package semantics evaluates generated execution contracts without mutating
// scheduler state. Observation precedence is:
//  1. EvaluationContext.contract_observation when present
//  2. SchedulingIntent.contract_observation otherwise
