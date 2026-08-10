# Communication-ready implementation proposals

These proposals translate current findings into bounded delivery slices. They
are recommendations, not claims of completed functionality or authorization to
change integration-owned files.

## Proposal A — One authoritative, fully populated pipeline

**Executive message:** The object model is complete, but execution profiles are
not equivalent. Consolidating the four missing standard Silver flows and using
them from local, Python, and CI runners would turn the current CI-specific happy
path into one reviewable contract.

**Implementation sequence:**

1. Specify Silver sales and ERP transformations from the mapping document.
2. Add reusable loaders with explicit truncate/idempotency behavior.
3. Replace the CI-only semantic fork with calls to canonical logic.
4. Define one ordered manifest and explicit `DataWarehouse` context.
5. Add source-to-layer row reconciliation, join-coverage, and non-empty Gold
   checks.
6. Update all execution documentation and retire divergent logic only after
   parity evidence.

**Definition of done:** clean bootstrap from an empty disposable SQL Server;
identical object/row contracts across supported runners; failures propagate;
static and runtime checks pass.

## Proposal B — Stable analytical contracts

**Executive message:** Current Gold objects are useful analytical views, but
their `ROW_NUMBER()` keys are temporary calculations rather than persisted
surrogates. Stabilize the contract before external BI consumers depend on those
values.

**Implementation sequence:** choose snapshot-stable natural/composite keys or
persisted dimensions; declare logical/physical constraints; make
`cust_is_future` canonical; verify rerun determinism and fan-out; publish a
consumer migration note.

**Definition of done:** key type and lifecycle documented, duplicate/null rules
enforced, fact-to-dimension coverage asserted, repeat runs preserve the stated
contract.

## Proposal C — Rights-clear synthetic fixture pack

**Executive message:** Replacing byte-identical course datasets with generated,
repository-owned synthetic fixtures removes ambiguity while retaining the
educational warehouse shape.

**Implementation sequence:** define a fixture schema and deterministic seed;
generate edge cases for every cleansing rule; record provenance/license; update
hashes and expected counts; validate semantic equivalence; remove inherited data
only after approval.

**Definition of done:** independent rights basis, deterministic regeneration,
documented scenario coverage, and a complete passing pipeline without inherited
CSV bytes.

## Proposal D — Safe object lifecycle and migrations

**Executive message:** Full database recreation is appropriate for the current
synthetic reference environment but is not a production migration strategy.
Introduce versioned, fail-closed migrations before claiming upgrade support.

**Implementation sequence:** baseline the current catalog; add migration IDs and
checksums; use `CREATE OR ALTER` where compatible; capture permissions and
rollback data; test upgrade and clean-install paths; apply the deprecation gates
in [`deprecation_and_removal.md`](../legacy/deprecation_and_removal.md).

**Definition of done:** clean install and supported upgrade yield the same
catalog, failed migrations stop execution, rollback evidence exists, and no
destructive action is inferred from a documentation candidate.

## Priority recommendation

Deliver Proposal A first. It closes the largest truth gap: the strongest path is
currently CI-specific. Proposal B follows because consumers should not adopt
unstable keys. Proposal C resolves reuse risk independently. Proposal D becomes
necessary before any real upgradeable environment is contemplated.
