/*
  Warnings:

  - A unique constraint covering the columns `[txHash]` on the table `Sessions` will be added. If there are existing duplicate values, this will fail.

*/
-- AlterTable
ALTER TABLE "Sessions" ADD COLUMN     "txHash" TEXT NOT NULL DEFAULT '0x';

-- CreateIndex
CREATE UNIQUE INDEX "Sessions_txHash_key" ON "Sessions"("txHash");
