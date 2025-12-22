-- DropIndex
DROP INDEX "Blocks_blockhash_key";

-- AlterTable
ALTER TABLE "Blocks" ALTER COLUMN "blockhash" DROP NOT NULL,
ALTER COLUMN "blockhash" SET DEFAULT '';

-- AlterTable
ALTER TABLE "Transactions" ADD COLUMN     "need_fix" BOOLEAN NOT NULL DEFAULT true;

-- CreateIndex
CREATE INDEX "DvpnTransaction_blocknumber_txhash_planId_idx" ON "DvpnTransaction"("blocknumber", "txhash", "planId");
