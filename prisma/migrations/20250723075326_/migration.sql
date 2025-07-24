/*
  Warnings:

  - You are about to drop the column `txHash` on the `Sessions` table. All the data in the column will be lost.
  - A unique constraint covering the columns `[txhash]` on the table `Sessions` will be added. If there are existing duplicate values, this will fail.
  - Added the required column `txhash` to the `Sessions` table without a default value. This is not possible if the table is not empty.

*/
-- DropIndex
DROP INDEX "Sessions_txHash_key";

-- AlterTable
ALTER TABLE "Sessions" DROP COLUMN "txHash",
ADD COLUMN     "txhash" TEXT NOT NULL;

-- CreateIndex
CREATE UNIQUE INDEX "Sessions_txhash_key" ON "Sessions"("txhash");
