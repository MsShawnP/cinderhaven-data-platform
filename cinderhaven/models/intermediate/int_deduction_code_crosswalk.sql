-- Deduction reason code crosswalk: resolves retailer-specific codes to
-- canonical deduction types and enriches with dispute rules.
--
-- Grain: one row per deduction code (code_id).
-- Joins deduction_codes → retailer_rules to attach dispute windows,
-- auto-deduct flags, and recovery rates to each code.

with codes as (
    select * from {{ ref('stg_deduction_codes') }}
),

rules as (
    select * from {{ ref('stg_retailer_rules') }}
),

retailers as (
    select * from {{ ref('stg_retailers') }}
),

crosswalk as (
    select
        codes.code_id,
        codes.retailer_id,
        retailers.name as retailer_name,
        codes.code,
        codes.name as code_name,
        codes.deduction_type,
        codes.is_published,
        rules.dispute_window_days,
        rules.auto_deduct,
        rules.evidence_required,
        rules.typical_recovery_rate
    from codes
    left join rules
        on codes.retailer_id = rules.retailer_id
        and codes.deduction_type = rules.deduction_type
    left join retailers
        on codes.retailer_id = retailers.retailer_id
)

select * from crosswalk
