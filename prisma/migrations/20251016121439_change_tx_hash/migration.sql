-- DropIndex
DROP INDEX "public"."DvpnTransaction_txhash_key";

-- AlterTable
ALTER TABLE "public"."DvpnTransaction" ALTER COLUMN "txhash" DROP NOT NULL;
