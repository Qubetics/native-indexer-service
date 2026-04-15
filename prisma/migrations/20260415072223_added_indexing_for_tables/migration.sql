/*
  Warnings:

  - A unique constraint covering the columns `[txhash]` on the table `DvpnTransaction` will be added. If there are existing duplicate values, this will fail.
  - Made the column `burnedFee` on table `Transactions` required. This step will fail if there are existing NULL values in that column.

*/
-- AlterTable
ALTER TABLE "Transactions" ALTER COLUMN "burnedFee" SET NOT NULL;

-- CreateIndex
CREATE UNIQUE INDEX "DvpnTransaction_txhash_key" ON "DvpnTransaction"("txhash");

-- CreateIndex
CREATE INDEX "Transactions_createdAt_idx" ON "Transactions"("createdAt");

-- CreateIndex
CREATE INDEX "Transactions_type_idx" ON "Transactions"("type");

-- CreateIndex
CREATE INDEX "Transactions_fromAddress_toAddress_idx" ON "Transactions"("fromAddress", "toAddress");

-- CreateIndex
CREATE INDEX "Users_createdAt_idx" ON "Users"("createdAt");

-- CreateIndex
CREATE INDEX "Users_address_idx" ON "Users"("address");
