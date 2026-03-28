# AI Phase 2 Merchandising Architecture

## Purpose

Phase 2 adds expert retail merchandising assistance on top of the Phase 1 planning platform.

The design goal is not generic AI chat. It is a domain-aware assistant that can produce grounded recommendations for:

- pricing
- suggested selling prices
- markdowns
- forecast adjustments
- merchandising rationale

## Required Inputs

- planning facts and deltas
- actual sales
- sold quantity
- sell-through
- starting inventory
- projected stock on hand
- department, class, and subclass context
- product lifecycle stage
- seasonality and event context
- price and markdown policy constraints
- supplier and replenishment constraints

## Expert Capability Targets

The solution should support an expert baby-products merchandising assistant that understands:

- category demand patterns by Department
- class and subclass substitution effects
- lifecycle sensitivity for infant and toddler products
- event and seasonality impacts
- margin floors and markdown guardrails
- inventory pressure and stock cover implications

## AI Service Pattern

Recommended pattern:

- structured recommendation service
- tool/function access to planning, pricing, inventory, and policy services
- human-in-the-loop approval before write-back

Recommended model integration:

- OpenAI Responses API
- structured outputs
- retrieval over product, policy, and planning context

## Outputs

The AI layer should return structured recommendation objects containing:

- recommendation type
- target scope
- proposed value
- rationale
- confidence
- business constraints applied
- expected impact
- warnings or exception conditions

## Phase Boundaries

Phase 1:

- capture all required data foundations
- expose APIs and schemas needed for later recommendation services

Phase 2:

- implement recommendation generation
- add review and approval UX
- add telemetry and evaluation loops
