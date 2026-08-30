package planexecutor

import (
	tgsrlv1 "github.com/Blizzard-cyber/TGS-RL/gen/go/tgsrl/v1"
	"github.com/Blizzard-cyber/TGS-RL/scheduler-go/state"
)

// Outcome is the terminal or resumable result of executing one durable Store
// transaction. The Store record is the only transaction state model.
type Outcome struct {
	Transaction *state.TransactionRecord
	Terminal    bool
}

// Results projects the latest provider effects into the protocol action-result
// surface used by DecisionRecord and Store finalization.
func (o *Outcome) Results() []*tgsrlv1.ActionResult {
	if o == nil || o.Transaction == nil {
		return nil
	}
	return transactionResults(o.Transaction.Plan, o.Transaction.State, o.Transaction.ProviderReceipt)
}

// Succeeded reports a fully committed transaction.
func (o *Outcome) Succeeded() bool {
	return o != nil && o.Terminal && o.Transaction != nil && o.Transaction.State == state.TransactionStateCommitted
}

// Degraded reports an unresolved or incompletely compensated side effect.
func (o *Outcome) Degraded() bool {
	return o != nil && o.Transaction != nil && o.Transaction.State == state.TransactionStateDegraded
}
