/*
  Warnings:

  - A unique constraint covering the columns `[address]` on the table `Rewards` will be added. If there are existing duplicate values, this will fail.

*/
-- DropIndex
DROP INDEX "Rewards_address_timeStamp_key";

-- CreateIndex
CREATE UNIQUE INDEX "Rewards_address_key" ON "Rewards"("address");
