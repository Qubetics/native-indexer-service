-- AlterTable: promote Delegator.id from INTEGER to BIGINT
-- The Delegator_id_seq sequence reached its 32-bit INTEGER ceiling (2147483647).
-- Changing the column to BIGINT and promoting the sequence to bigint allows
-- values up to 9223372036854775807 without touching existing rows.
ALTER TABLE "Delegator" ALTER COLUMN "id" TYPE BIGINT;
ALTER SEQUENCE "Delegator_id_seq" AS bigint;

-- AlterTable: promote ValidatorStake.id from INTEGER to BIGINT
-- ValidatorStake has a 1-to-many relationship with Delegator and is equally
-- at risk of hitting the same ceiling under high indexing load.
ALTER TABLE "ValidatorStake" ALTER COLUMN "id" TYPE BIGINT;
ALTER SEQUENCE "ValidatorStake_id_seq" AS bigint;
